// @ts-nocheck
import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';

import type { CsvFileOption, GroupedListingRow } from '$lib/types/csv-viewer';

const OUTPUTS_DIR = path.resolve(process.cwd(), '../outputs/normalized');

type CsvRecord = Record<string, string>;

type CacheEntry = {
	mtimeMs: number;
	rows: GroupedListingRow[];
};

const datasetCache = new Map<string, CacheEntry>();

function getOutputsDir() {
	return OUTPUTS_DIR;
}

function safeCsvFilename(filename: string) {
	if (!/^[\w.-]+\.csv$/i.test(filename)) {
		throw new Error('Invalid CSV filename.');
	}

	return filename;
}

function parseCsv(text: string) {
	const rows: string[][] = [];
	let currentRow: string[] = [];
	let currentCell = '';
	let inQuotes = false;

	for (let index = 0; index < text.length; index += 1) {
		const character = text[index];
		const nextCharacter = text[index + 1];

		if (character === '"') {
			if (inQuotes && nextCharacter === '"') {
				currentCell += '"';
				index += 1;
			} else {
				inQuotes = !inQuotes;
			}
			continue;
		}

		if (character === ',' && !inQuotes) {
			currentRow.push(currentCell);
			currentCell = '';
			continue;
		}

		if ((character === '\n' || character === '\r') && !inQuotes) {
			if (character === '\r' && nextCharacter === '\n') {
				index += 1;
			}
			currentRow.push(currentCell);
			rows.push(currentRow);
			currentRow = [];
			currentCell = '';
			continue;
		}

		currentCell += character;
	}

	if (currentCell.length > 0 || currentRow.length > 0) {
		currentRow.push(currentCell);
		rows.push(currentRow);
	}

	const cleanedRows = rows.filter((row) =>
		row.some((value) => value.trim() !== '')
	);
	if (cleanedRows.length === 0) {
		return [];
	}

	const [headerRow, ...dataRows] = cleanedRows;
	const headers = headerRow.map((header) => header.trim());

	return dataRows.map<CsvRecord>((row) => {
		const record: CsvRecord = {};
		headers.forEach((header, headerIndex) => {
			record[header] = (row[headerIndex] ?? '').trim();
		});
		return record;
	});
}

function toNumber(value: string) {
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : Number.POSITIVE_INFINITY;
}

function listingGroupKey(row: CsvRecord) {
	return [
		row.source_file,
		row.vendor_name,
		row.captured_at,
		row.item_name,
		row.category_path,
		row.stock_text
	].join('||');
}

function createListingId(groupKey: string) {
	return createHash('sha1').update(groupKey).digest('hex').slice(0, 12);
}

function groupListingRows(rows: CsvRecord[]) {
	const grouped = new Map<string, GroupedListingRow>();
	const tierSeen = new Map<string, Set<string>>();

	for (const row of rows) {
		const groupKey = listingGroupKey(row);
		const tierKey = [
			row.quantity_from,
			row.unit,
			row.currency,
			row.price_per_unit
		].join('||');

		if (!grouped.has(groupKey)) {
			grouped.set(groupKey, {
				id: createListingId(groupKey),
				source_file: row.source_file ?? '',
				vendor_name: row.vendor_name ?? '',
				bot_handle: row.bot_handle ?? '',
				captured_at: row.captured_at ?? '',
				item_name: row.item_name ?? '',
				category_path: row.category_path ?? '',
				normalized_category_path: row.normalized_category_path ?? '',
				normalized_category_root: row.normalized_category_root ?? '',
				normalized_category_leaf: row.normalized_category_leaf ?? '',
				normalization_reason: row.normalization_reason ?? '',
				stock_text: row.stock_text ?? '',
				stock_quantity: row.stock_quantity ?? '',
				currency: row.currency ?? '',
				tier_count: 0,
				pricing_tiers: []
			});
			tierSeen.set(groupKey, new Set<string>());
		}

		const listing = grouped.get(groupKey);
		const tiers = tierSeen.get(groupKey);
		if (!listing || !tiers || tiers.has(tierKey)) {
			continue;
		}

		tiers.add(tierKey);
		listing.pricing_tiers.push({
			quantity_from: row.quantity_from ?? '',
			unit: row.unit ?? '',
			currency: row.currency ?? '',
			price_per_unit: row.price_per_unit ?? ''
		});
	}

	const listings = [...grouped.values()];
	for (const listing of listings) {
		listing.pricing_tiers.sort((left, right) => {
			const quantityDelta =
				toNumber(left.quantity_from) - toNumber(right.quantity_from);
			if (quantityDelta !== 0) {
				return quantityDelta;
			}
			return toNumber(left.price_per_unit) - toNumber(right.price_per_unit);
		});
		listing.tier_count = listing.pricing_tiers.length;
	}

	listings.sort((left, right) => {
		const vendorDelta = left.vendor_name.localeCompare(
			right.vendor_name,
			undefined,
			{
				numeric: true,
				sensitivity: 'base'
			}
		);
		if (vendorDelta !== 0) {
			return vendorDelta;
		}

		const itemDelta = left.item_name.localeCompare(right.item_name, undefined, {
			numeric: true,
			sensitivity: 'base'
		});
		if (itemDelta !== 0) {
			return itemDelta;
		}

		return right.captured_at.localeCompare(left.captured_at);
	});

	return listings;
}

export async function listCsvFiles(): Promise<CsvFileOption[]> {
	const entries = await fs.readdir(getOutputsDir(), { withFileTypes: true });
	return entries
		.filter(
			(entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.csv')
		)
		.map((entry) => ({ name: entry.name }))
		.sort((left, right) => left.name.localeCompare(right.name));
}

export async function getDefaultCsvFilename() {
	const files = await listCsvFiles();
	if (files.length === 0) {
		return null;
	}

	return (
		files.find((file) => file.name === 'downloads_flat.csv')?.name ??
		files[0].name
	);
}

export async function loadGroupedCsvDataset(filename: string) {
	const safeFilename = safeCsvFilename(filename);
	const filePath = path.join(getOutputsDir(), safeFilename);
	const stat = await fs.stat(filePath);
	const cached = datasetCache.get(safeFilename);
	if (cached && cached.mtimeMs === stat.mtimeMs) {
		return cached.rows;
	}

	const fileText = await fs.readFile(filePath, 'utf-8');
	const parsedRows = parseCsv(fileText);
	const groupedRows = groupListingRows(parsedRows);
	datasetCache.set(safeFilename, { mtimeMs: stat.mtimeMs, rows: groupedRows });
	return groupedRows;
}

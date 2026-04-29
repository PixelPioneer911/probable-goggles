import { error, json } from '@sveltejs/kit';

import { listCsvFiles, loadGroupedCsvDataset } from '$lib/server/csv-viewer';

export async function GET({ params }) {
	const filename = params.filename ?? '';
	const csvFiles = await listCsvFiles();
	const fileExists = csvFiles.some((file) => file.name === filename);

	if (!fileExists) {
		throw error(404, 'CSV file not found.');
	}

	try {
		const rows = await loadGroupedCsvDataset(filename);
		return json({
			filename,
			rows
		});
	} catch (caught) {
		const message =
			caught instanceof Error ? caught.message : 'Could not load CSV file.';
		throw error(400, message);
	}
}

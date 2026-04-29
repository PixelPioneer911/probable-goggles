<script lang="ts">
import ChevronDown from '@lucide/svelte/icons/chevron-down';
import Search from '@lucide/svelte/icons/search';
import { untrack } from 'svelte';

import type { GroupedListingRow } from '$lib/types/csv-viewer';

import type { PageData } from './$types';

type ViewerResponse = {
	filename: string;
	rows: GroupedListingRow[];
};

type SortKey =
	| 'item_name'
	| 'bot_handle'
	| 'normalized_category_path'
	| 'stock_text'
	| 'currency'
	| 'captured_at';

const CHUNK_SIZE = 50;

const { data }: { data: PageData } = $props();

const selectedCsv = $state(untrack(() => data.defaultCsv));
let rows = $state<GroupedListingRow[]>([]);
let loading = $state(false);
let loadError = $state('');
let searchTerm = $state('');
let vendorFilter = $state('');
let rootFilter = $state('');
let leafFilter = $state('');
let currencyFilter = $state('');
let visibleCount = $state(CHUNK_SIZE);
let expandedRowIds = $state<Set<string>>(new Set());
// biome-ignore lint/style/useConst: bind:this requires a mutable binding target.
let sentinelElement = $state<HTMLDivElement | null>(null);
let activeRequest = 0;
let compactFilters = $state(false);
let sortKey = $state<SortKey | null>(null);
let sortDirection = $state<'asc' | 'desc'>('asc');

function formatCount(value: number) {
	return new Intl.NumberFormat('en-GB').format(value);
}

function formatTimestamp(value: string) {
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) {
		return value;
	}

	return new Intl.DateTimeFormat('en-GB', {
		dateStyle: 'medium',
		timeStyle: 'short'
	}).format(date);
}

function formatPrice(value: string, currency: string) {
	if (!value) {
		return '—';
	}
	return `${currency}${value}`;
}

function uniqueOptions(values: string[]) {
	return [...new Set(values.filter(Boolean))]
		.sort((left, right) =>
			left.localeCompare(right, undefined, { sensitivity: 'base' })
		)
		.map((value) => ({ label: value, value }));
}

function resetFilters() {
	searchTerm = '';
	vendorFilter = '';
	rootFilter = '';
	leafFilter = '';
	currencyFilter = '';
	visibleCount = CHUNK_SIZE;
	expandedRowIds = new Set();
}

function toNumericValue(value: string) {
	const parsed = Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function stockSortValue(row: GroupedListingRow) {
	if (row.stock_text.trim().toLowerCase() === 'unlimited') {
		return Number.POSITIVE_INFINITY;
	}

	const stockQuantity = toNumericValue(row.stock_quantity);
	if (stockQuantity !== null) {
		return stockQuantity;
	}

	return Number.NEGATIVE_INFINITY;
}

function compareRows(left: GroupedListingRow, right: GroupedListingRow) {
	if (!sortKey) {
		return 0;
	}

	if (sortKey === 'captured_at') {
		return left.captured_at.localeCompare(right.captured_at, undefined, {
			numeric: true,
			sensitivity: 'base'
		});
	}

	if (sortKey === 'stock_text') {
		const leftStock = stockSortValue(left);
		const rightStock = stockSortValue(right);
		if (leftStock !== rightStock) {
			return leftStock - rightStock;
		}
	}

	const leftValue = (left[sortKey] ?? '').toString();
	const rightValue = (right[sortKey] ?? '').toString();
	return leftValue.localeCompare(rightValue, undefined, {
		numeric: true,
		sensitivity: 'base'
	});
}

function toggleSort(nextKey: SortKey) {
	if (sortKey === nextKey) {
		sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
	} else {
		sortKey = nextKey;
		sortDirection = nextKey === 'captured_at' ? 'desc' : 'asc';
	}

	visibleCount = CHUNK_SIZE;
	expandedRowIds = new Set();
}

function sortIndicator(key: SortKey) {
	if (sortKey !== key) {
		return '↕';
	}

	return sortDirection === 'asc' ? '↑' : '↓';
}

function toggleExpanded(rowId: string) {
	const next = new Set(expandedRowIds);
	if (next.has(rowId)) {
		next.delete(rowId);
	} else {
		next.add(rowId);
	}
	expandedRowIds = next;
}

function handleRowKeydown(event: KeyboardEvent, rowId: string) {
	if (event.key !== 'Enter' && event.key !== ' ') {
		return;
	}
	event.preventDefault();
	toggleExpanded(rowId);
}

function buildRowSearchText(row: GroupedListingRow) {
	return [
		row.item_name,
		row.vendor_name,
		row.bot_handle,
		row.category_path,
		row.normalized_category_path,
		row.stock_text,
		row.currency
	]
		.join(' ')
		.toLowerCase();
}

const vendorOptions = $derived(
	uniqueOptions(rows.map((row) => row.vendor_name))
);
const rootOptions = $derived(
	uniqueOptions(rows.map((row) => row.normalized_category_root))
);
const leafOptions = $derived(
	uniqueOptions(rows.map((row) => row.normalized_category_leaf))
);
const currencyOptions = $derived(
	uniqueOptions(rows.map((row) => row.currency))
);

const filteredRows = $derived.by(() => {
	const query = searchTerm.trim().toLowerCase();

	return rows.filter((row) => {
		if (vendorFilter && row.vendor_name !== vendorFilter) {
			return false;
		}
		if (rootFilter && row.normalized_category_root !== rootFilter) {
			return false;
		}
		if (leafFilter && row.normalized_category_leaf !== leafFilter) {
			return false;
		}
		if (currencyFilter && row.currency !== currencyFilter) {
			return false;
		}
		if (query && !buildRowSearchText(row).includes(query)) {
			return false;
		}
		return true;
	});
});

const sortedRows = $derived.by(() => {
	const nextRows = [...filteredRows];
	if (!sortKey) {
		return nextRows;
	}

	nextRows.sort((left, right) => {
		const result = compareRows(left, right);
		return sortDirection === 'asc' ? result : -result;
	});
	return nextRows;
});

const displayedRows = $derived(sortedRows.slice(0, visibleCount));
const hasMoreRows = $derived(visibleCount < sortedRows.length);
async function loadCsv(filename: string) {
	activeRequest += 1;
	const requestId = activeRequest;
	loading = true;
	loadError = '';

	try {
		const response = await fetch(`/api/csv/${encodeURIComponent(filename)}`);
		if (!response.ok) {
			const message = await response.text();
			throw new Error(message || `Request failed with ${response.status}`);
		}

		const payload = (await response.json()) as ViewerResponse;
		if (requestId !== activeRequest) {
			return;
		}

		rows = payload.rows;
		visibleCount = CHUNK_SIZE;
		expandedRowIds = new Set();
	} catch (caught) {
		if (requestId !== activeRequest) {
			return;
		}

		rows = [];
		loadError =
			caught instanceof Error
				? caught.message
				: 'Could not load the CSV dataset.';
	} finally {
		if (requestId === activeRequest) {
			loading = false;
		}
	}
}

function loadMoreRows() {
	if (!hasMoreRows || loading) {
		return;
	}
	visibleCount = Math.min(visibleCount + CHUNK_SIZE, sortedRows.length);
}

function handleWindowScroll() {
	compactFilters = window.scrollY > 24;
}

$effect(() => {
	void loadCsv(selectedCsv);
});

$effect(() => {
	searchTerm;
	vendorFilter;
	rootFilter;
	leafFilter;
	currencyFilter;
	sortKey;
	sortDirection;
	visibleCount = CHUNK_SIZE;
	expandedRowIds = new Set();
});

$effect(() => {
	if (!sentinelElement) {
		return;
	}

	const observer = new IntersectionObserver(
		(entries) => {
			if (entries[0]?.isIntersecting) {
				loadMoreRows();
			}
		},
		{
			rootMargin: '240px 0px'
		}
	);

	observer.observe(sentinelElement);

	return () => observer.disconnect();
});
</script>

<svelte:head>
	<title>CSV Viewer</title>
</svelte:head>

<svelte:window onscroll={handleWindowScroll} />

<div class="min-h-screen bg-background text-foreground">
	<div class="flex min-h-screen flex-col px-2 py-2 sm:px-3 sm:py-3">
		<section
			class={`sticky z-20 mb-2 rounded-xl border border-border/80 bg-card/92 text-card-foreground shadow-sm backdrop-blur transition-[top,width,margin,transform,background-color] duration-300 ease-out ${
				compactFilters
					? 'top-3 mx-auto w-[98%] sm:w-[97%] lg:w-[96%]'
					: 'top-0 mx-0 w-full'
			}`}
		>
			<div class="grid gap-2 p-2.5 xl:grid-cols-[1.85fr_1fr_1fr_1fr_0.8fr_auto]">
				<label class="space-y-1">
					<span class="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground"
						>Search</span
					>
					<div class="relative">
						<Search class="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
						<input
							class="flex h-9 w-full rounded-md border border-border/80 bg-background py-2 pl-9 pr-3 text-sm shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
							type="search"
							placeholder="Item, vendor, category..."
							bind:value={searchTerm}
						/>
					</div>
				</label>

				<label class="space-y-1">
					<span class="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground"
						>Vendor</span
					>
					<select
						class="flex h-9 w-full rounded-md border border-border/80 bg-background px-3 py-2 text-sm shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
						bind:value={vendorFilter}
					>
						<option value="">All vendors</option>
						{#each vendorOptions as option}
							<option value={option.value}>{option.label}</option>
						{/each}
					</select>
				</label>

				<label class="space-y-1">
					<span class="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground"
						>Category</span
					>
					<select
						class="flex h-9 w-full rounded-md border border-border/80 bg-background px-3 py-2 text-sm shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
						bind:value={rootFilter}
					>
						<option value="">All categories</option>
						{#each rootOptions as option}
							<option value={option.value}>{option.label}</option>
						{/each}
					</select>
				</label>

				<label class="space-y-1">
					<span class="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground"
						>Subcategory</span
					>
					<select
						class="flex h-9 w-full rounded-md border border-border/80 bg-background px-3 py-2 text-sm shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
						bind:value={leafFilter}
					>
						<option value="">All subcategories</option>
						{#each leafOptions as option}
							<option value={option.value}>{option.label}</option>
						{/each}
					</select>
				</label>

				<label class="space-y-1">
					<span class="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground"
						>Currency</span
					>
					<select
						class="flex h-9 w-full rounded-md border border-border/80 bg-background px-3 py-2 text-sm shadow-xs outline-none transition-colors focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
						bind:value={currencyFilter}
					>
						<option value="">All currencies</option>
						{#each currencyOptions as option}
							<option value={option.value}>{option.label}</option>
						{/each}
					</select>
				</label>

				<div class="flex items-end">
					<button
						class="inline-flex h-9 w-full items-center justify-center rounded-md border border-border/80 bg-background px-4 text-sm font-medium shadow-xs transition-colors hover:bg-accent hover:text-accent-foreground"
						type="button"
						onclick={resetFilters}
					>
						Reset
					</button>
				</div>
			</div>
			<div class="border-t border-border/70 px-3 py-1.5 text-sm text-muted-foreground">
				{#if loading}
					Loading latest listings...
				{:else if loadError}
					<span class="font-medium text-destructive">{loadError}</span>
				{:else}
					<span class="font-medium text-foreground">{formatCount(filteredRows.length)}</span>
					items
				{/if}
			</div>
		</section>

		<section
			class="flex min-h-0 flex-1 overflow-hidden rounded-xl border border-border/80 bg-card shadow-sm"
		>
			<div class="min-h-0 w-full overflow-auto">
				<table class="min-w-full border-collapse text-sm">
					<thead class="sticky top-0 z-10 bg-muted/60 text-left backdrop-blur">
						<tr class="border-b">
							<th class="w-10 px-4 py-3"></th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('item_name')}
								>
									Item
									<span class="text-xs text-muted-foreground">{sortIndicator('item_name')}</span>
								</button>
							</th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('bot_handle')}
								>
									Vendor
									<span class="text-xs text-muted-foreground">{sortIndicator('bot_handle')}</span>
								</button>
							</th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('normalized_category_path')}
								>
									Category
									<span class="text-xs text-muted-foreground">
										{sortIndicator('normalized_category_path')}
									</span>
								</button>
							</th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('stock_text')}
								>
									Stock
									<span class="text-xs text-muted-foreground">{sortIndicator('stock_text')}</span>
								</button>
							</th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('currency')}
								>
									Currency
									<span class="text-xs text-muted-foreground">{sortIndicator('currency')}</span>
								</button>
							</th>
							<th class="px-4 py-3 font-medium">
								<button
									class="inline-flex items-center gap-1 transition-colors hover:text-foreground"
									type="button"
									onclick={() => toggleSort('captured_at')}
								>
									Captured
									<span class="text-xs text-muted-foreground">{sortIndicator('captured_at')}</span>
								</button>
							</th>
						</tr>
					</thead>
					<tbody>
						{#if !loading && !loadError && displayedRows.length === 0}
							<tr>
								<td colspan="7" class="px-4 py-12 text-center text-muted-foreground">
									No grouped listings match the current filters.
								</td>
							</tr>
						{/if}

						{#each displayedRows as row (row.id)}
							<tr
								class="cursor-pointer border-b border-border/70 transition-colors hover:bg-muted/30"
								role="button"
								tabindex="0"
								aria-expanded={expandedRowIds.has(row.id)}
								onclick={() => toggleExpanded(row.id)}
								onkeydown={(event) => handleRowKeydown(event, row.id)}
							>
								<td class="px-4 py-3 align-top">
									<div
										class={`inline-flex size-7 items-center justify-center rounded-full border bg-background transition-transform ${expandedRowIds.has(row.id) ? 'rotate-180' : ''}`}
									>
										<ChevronDown class="size-4" />
									</div>
								</td>
								<td class="px-4 py-3 align-top">
									<p class="font-medium">{row.item_name}</p>
								</td>
								<td class="px-4 py-3 align-top">
									<p class="font-medium">@{row.bot_handle}</p>
								</td>
								<td class="px-4 py-3 align-top">
									<p class="font-medium">{row.normalized_category_path || row.category_path}</p>
								</td>
								<td class="px-4 py-3 align-top">
									<p class="font-medium">{row.stock_text || '—'}</p>
								</td>
								<td class="px-4 py-3 align-top">
									<p class="font-medium">{row.currency || '—'}</p>
								</td>
								<td class="px-4 py-3 align-top text-muted-foreground">
									{formatTimestamp(row.captured_at)}
								</td>
							</tr>

							{#if expandedRowIds.has(row.id)}
								<tr class="border-b border-border/70 bg-muted/10">
									<td colspan="7" class="px-4 py-4">
										<div class="rounded-xl border border-border/80 bg-background">
											<div class="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
												<div>
													<h2 class="text-sm font-semibold">Pricing tiers</h2>
													<div class="mt-1 space-y-1 text-xs text-muted-foreground">
														<p>{row.item_name} · {row.vendor_name}</p>
														<p>Category: {row.category_path}</p>
													</div>
												</div>
												<div class="text-xs text-muted-foreground">{row.tier_count} tiers</div>
											</div>

											<div class="overflow-x-auto">
												<table class="min-w-full text-sm">
													<thead class="bg-muted/30 text-left">
														<tr class="border-b">
															<th class="px-4 py-2.5 font-medium">Quantity from</th>
															<th class="px-4 py-2.5 font-medium">Unit</th>
															<th class="px-4 py-2.5 font-medium">Currency</th>
															<th class="px-4 py-2.5 font-medium">Price per unit</th>
														</tr>
													</thead>
													<tbody>
														{#each row.pricing_tiers as tier}
															<tr class="border-b last:border-b-0">
																<td class="px-4 py-2.5">{tier.quantity_from}</td>
																<td class="px-4 py-2.5">{tier.unit}</td>
																<td class="px-4 py-2.5">{tier.currency}</td>
																<td class="px-4 py-2.5 font-medium">
																	{formatPrice(tier.price_per_unit, tier.currency)}
																</td>
															</tr>
														{/each}
													</tbody>
												</table>
											</div>
										</div>
									</td>
								</tr>
							{/if}
						{/each}
					</tbody>
				</table>
			</div>
		</section>

		{#if !loadError && filteredRows.length > 0}
			<div bind:this={sentinelElement} class="flex justify-center py-1">
				{#if hasMoreRows}
					<div class="rounded-full border bg-background px-3 py-1 text-xs text-muted-foreground">
						Loading more rows as you scroll…
					</div>
				{:else}
					<div class="rounded-full border bg-background px-3 py-1 text-xs text-muted-foreground">
						All grouped listings are visible.
					</div>
				{/if}
			</div>
		{/if}
	</div>
</div>

export type CsvFileOption = {
	name: string;
};

export type PricingTier = {
	quantity_from: string;
	unit: string;
	currency: string;
	price_per_unit: string;
};

export type GroupedListingRow = {
	id: string;
	source_file: string;
	vendor_name: string;
	bot_handle: string;
	captured_at: string;
	item_name: string;
	category_path: string;
	normalized_category_path: string;
	normalized_category_root: string;
	normalized_category_leaf: string;
	normalization_reason: string;
	stock_text: string;
	stock_quantity: string;
	currency: string;
	tier_count: number;
	pricing_tiers: PricingTier[];
};

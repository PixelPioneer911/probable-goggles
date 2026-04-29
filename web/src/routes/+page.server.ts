import { error } from '@sveltejs/kit';

import { getDefaultCsvFilename, listCsvFiles } from '$lib/server/csv-viewer';

export async function load() {
	const csvFiles = await listCsvFiles();
	const defaultCsv = await getDefaultCsvFilename();

	if (csvFiles.length === 0 || !defaultCsv) {
		throw error(404, 'No CSV files were found in outputs/normalized.');
	}

	return {
		defaultCsv
	};
}

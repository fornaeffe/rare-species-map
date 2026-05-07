import { copyFile, mkdir, stat } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(here, '..');
const repoRoot = resolve(webRoot, '..');
const source = resolve(repoRoot, 'data/tiles/rare_species_cells.pmtiles');
const target = resolve(webRoot, 'static/tiles/rare_species_cells.pmtiles');

await stat(source);
await mkdir(dirname(target), { recursive: true });
await copyFile(source, target);

console.log(`Copied ${source} -> ${target}`);

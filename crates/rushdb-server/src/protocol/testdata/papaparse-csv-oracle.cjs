// Reference outputs for the engine's CSV decoding and export (protocol/csv.rs).
//
// This is the platform's CSV import (Papa Parse 5.4.1 `parse`, then the header
// clean-up of import.controller.ts) and CSV export (`unparse` of export.controller.ts)
// as they ran before CSV moved into the engine. csv_tests.rs asserts that the engine
// reproduces every case. Regenerate after adding a case:
//
//   node papaparse-csv-oracle.cjs /path/to/node_modules/papaparse > papaparse-csv-oracle.json
const { parse, unparse } = require(process.argv[2] ?? 'papaparse')
function coreCsv(data, options = {}, parseConfig = {}) {
  const defaultConfig = { header: true, delimiter: ',', skipEmptyLines: true, dynamicTyping: options.suggestTypes ?? false }
  const config = Object.assign({}, defaultConfig, parseConfig, {
    dynamicTyping: typeof parseConfig.dynamicTyping === 'boolean' ? parseConfig.dynamicTyping : defaultConfig.dynamicTyping
  })
  const result = parse(data, config)
  if (result.errors?.length) return { error: `CSV parse error: ${result.errors[0].message}` }
  const originalFields = result?.meta?.fields || []
  const seen = new Set(); const fieldReplacements = {}
  originalFields.forEach((raw, idx) => {
    let name = (raw ?? '').replace(/\0/g, '').trim()
    if (!name) name = `column_${idx + 1}`
    name = name.replace(/\s+/g, '_')
    if (!/^[A-Za-z_]/.test(name)) name = `_${name}`
    if (seen.has(name)) { const base = name; let c = 2; while (seen.has(`${base}_${c}`)) c++; name = `${base}_${c}` }
    seen.add(name); fieldReplacements[raw] = name
  })
  const rows = result.data.map((draft) => {
    if (draft && typeof draft === 'object' && !Array.isArray(draft)) {
      return Object.fromEntries(Object.entries(draft).filter(([k, v]) => v !== null && v !== undefined && k !== '').map(([k, v]) => [fieldReplacements[k] ?? k, v]).filter(([k]) => k !== ''))
    }
    if (Array.isArray(draft)) return draft.reduce((acc, val, i) => { acc[`column_${i + 1}`] = val; return acc }, {})
    return {}
  })
  return { rows: JSON.parse(JSON.stringify(rows)) }
}
const cases = [
  ['basic', 'name,age\nAda,36\nAlan,41\n', {}, {}],

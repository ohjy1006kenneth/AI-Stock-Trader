/**
 * chart_test_harness.mjs
 *
 * B2-REV-002: Behavior test harness for renderTrainingRegimeChart.
 *
 * Extracts the chart function + helpers from a rendered dashboard HTML file,
 * runs the function with a mock DOM and fixture payload, and returns a JSON
 * report of the resulting chart elements for assertion by pytest.
 *
 * Usage:
 *   node chart_test_harness.mjs <html_file> <payload_json>
 *
 * Exit 0 on success, prints JSON report to stdout.
 * Exit 1 on failure, prints error to stderr.
 */

import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';

const htmlPath = process.argv[2];
const payloadPath = process.argv[3];

if (!htmlPath || !payloadPath) {
  process.stderr.write('Usage: node chart_test_harness.mjs <html_file> <payload_json>\n');
  process.exit(1);
}

const html = fs.readFileSync(htmlPath, 'utf-8');
const payload = JSON.parse(fs.readFileSync(payloadPath, 'utf-8'));

// ── Extract the JS block from the <script> tag ──
const scriptMatch = html.match(/<script[^>]*>([\s\S]*)<\/script>/i);
if (!scriptMatch) {
  process.stderr.write('No <script> tag found in HTML\n');
  process.exit(1);
}

const jsSource = scriptMatch[1];

// ── Extract a function by name (handles nested braces) ──
function extractFunction(name, source) {
  const regex = new RegExp(`function\\s+${name}\\s*\\([^)]*\\)\\s*\\{`);
  const match = regex.exec(source);
  if (!match) return null;

  let depth = 0;
  let i = match.index + match[0].length - 1; // position at opening {
  for (; i < source.length; i++) {
    if (source[i] === '{') depth++;
    if (source[i] === '}') depth--;
    if (depth === 0) break;
  }
  return source.substring(match.index, i + 1);
}

const badgeFn = extractFunction('badge', jsSource);
const escapeHtmlFn = extractFunction('escapeHtml', jsSource);
const formatNumberFn = extractFunction('formatNumber', jsSource);
const chartFn = extractFunction('renderTrainingRegimeChart', jsSource);

if (!badgeFn) { process.stderr.write('Could not extract badge function\n'); process.exit(1); }
if (!escapeHtmlFn) { process.stderr.write('Could not extract escapeHtml function\n'); process.exit(1); }
if (!formatNumberFn) { process.stderr.write('Could not extract formatNumber function\n'); process.exit(1); }
if (!chartFn) { process.stderr.write('Could not extract renderTrainingRegimeChart function\n'); process.exit(1); }

// ── Build and run a standalone script ──
const tmpFile = path.join(path.dirname(htmlPath), `chart_run_${Date.now()}.mjs`);

const script = `
// Mock DOM elements
const trainingRegimeMetaEl = { innerHTML: '' };
const trainingRegimeChartEl = { className: '', innerHTML: '' };
const errors = [];
const payloadJson = ${JSON.stringify(JSON.stringify(payload))};

// Inject helpers
${badgeFn}
${escapeHtmlFn}
${formatNumberFn}

// Inject chart function (already references the mock DOM consts above)
${chartFn}

// Run the chart
try {
  renderTrainingRegimeChart(JSON.parse(payloadJson));
} catch(e) {
  errors.push(e.message);
}

// Analyze SVG output
let svgAnalysis = null;
const chartContent = trainingRegimeChartEl.innerHTML || '';
if (typeof chartContent === 'string' && chartContent.includes('<svg')) {
  const svg = chartContent;

  // Count polyline segments
  const polylineMatches = svg.match(/<polyline[^>]*class="probability-line"[^>]*>/g) || [];
  const polylines = polylineMatches.map(p => {
    const pointsMatch = p.match(/points="([^"]*)"/);
    const points = pointsMatch ? pointsMatch[1].split(' ').filter(Boolean) : [];
    return { pointsCount: points.length };
  });

  // Count gap markers
  const gapMarkerMatches = svg.match(/<polygon[^>]*class="gap-marker"[^>]*>/g) || [];

  svgAnalysis = {
    hasSvg: true,
    polylineCount: polylines.length,
    gapMarkerCount: gapMarkerMatches.length,
    validSegments: polylines.filter(p => p.pointsCount > 1).length,
    polylines: polylines,
  };
}

// Parse meta badges (extract "label value" from badge spans)
const badgeRegex = new RegExp('<span[^>]*class="[^"]*badge[^"]*"[^>]*>.*?<strong>([^<]*)</strong>\\s*([^<]*)</span>', 'g');
const metaStr = trainingRegimeMetaEl.innerHTML || '';
const metaBadges = [];
let m;
while ((m = badgeRegex.exec(metaStr)) !== null) {
  metaBadges.push((m[1] || '').trim() + ' ' + (m[2] || '').trim());
}

const result = {
  chartClass: trainingRegimeChartEl.className || '',
  chartHtml: chartContent.substring(0, 20000),
  metaBadges: metaBadges,
  svgAnalysis: svgAnalysis,
  blockerHtml: svgAnalysis ? null : chartContent.substring(0, 500),
  errors: errors,
};

console.log(JSON.stringify(result));
`;

fs.writeFileSync(tmpFile, script);
try {
  const output = execFileSync('node', [tmpFile], { encoding: 'utf-8', timeout: 10000 });
  const result = JSON.parse(output.trim());
  console.log(JSON.stringify(result));
} finally {
  try { fs.unlinkSync(tmpFile); } catch(_) {}
}

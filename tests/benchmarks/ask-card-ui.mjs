import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { chromium } from "playwright";
import { askCardCases } from "./ask-card-cases.ts";
import { uxCardCases } from "./ux-card-cases.ts";
import { cardStats } from "../../src/octo/cards.ts";
import { parseAskActionId, resolveAskAnswer, resolveAskSetAnswer } from "../../src/cards/ask-card.ts";

// Pinned local renderer and Chromium; no HTTP requests, credentials, model or clock inputs.
// A geometry proxy, NOT an aesthetic rating. A keep also requires main-agent screenshot review.
// Score = mean initial card height + 1000 per overflowing element + 50 per undersized action.
// Expanded states also contribute overflow/action penalties, but not initial-height cost.
// Host CSS and scenarios are frozen. A lower score must preserve content and pass human review.
const samples = [...askCardCases, ...uxCardCases];
assert.equal(samples.length, 21, "The fixed UX matrix must stay complete");
assert.equal(new Set(samples.map(sample => sample.id)).size, samples.length);
const out = join(process.env.TMPDIR || "/tmp", "oco-ux-card-benchmark");
await mkdir(out, { recursive: true });
const renderer = await readFile("node_modules/adaptivecards/dist/adaptivecards.min.js", "utf8");
const markdown = await readFile("node_modules/markdown-it/dist/markdown-it.min.js", "utf8");
const styles = await readFile("node_modules/adaptivecards/dist/adaptivecards.css", "utf8");
const script = `
const samples = ${JSON.stringify(samples)};
const md = window.markdownit({html:false});
AdaptiveCards.AdaptiveCard.onProcessMarkdown = (text, result) => { result.outputHtml = md.render(text); result.didProcess = true; };
window.submissions = [];
window.renderErrors = [];
window.cards = {};
const params = new URLSearchParams(location.search);
const width = Number(params.get('w')) || 440;
const only = params.get('only');
for (const sample of samples) {
  if (only && !only.split(',').includes(sample.id)) continue;
  const section = document.createElement('section'); section.id = sample.id;
  const label = document.createElement('h2'); label.textContent = sample.id; section.append(label);
  const bubble = document.createElement('div'); bubble.className = 'bubble'; bubble.style.width = width + 'px'; section.append(bubble);
  const card = new AdaptiveCards.AdaptiveCard();
  card.hostConfig = new AdaptiveCards.HostConfig({
    supportsInteractivity:true, fontFamily:'Arial, PingFang SC, sans-serif',
    fontSizes:{small:12,default:14,medium:17,large:21,extraLarge:26},
    spacing:{small:4,default:8,medium:12,large:16,extraLarge:20,padding:16},
    actions:{maxActions:20,actionsOrientation:'horizontal',actionAlignment:'left',buttonSpacing:8},
    containerStyles:{default:{backgroundColor:'#FFFFFF'},emphasis:{backgroundColor:'#F2F3F5'},accent:{backgroundColor:'#F0ECFF'},good:{backgroundColor:'#EEF8F0'},attention:{backgroundColor:'#FFF0F0'},warning:{backgroundColor:'#FFF8E6'}}
  });
  card.onExecuteAction = action => window.submissions.push({sample:sample.id,id:action.id,inputs:action.data});
  card.parse(sample.frame.card);
  const failures = card.validateProperties().validationEvents;
  if (failures.length) window.renderErrors.push({sample:sample.id,errors:failures.map(x=>x.message)});
  bubble.append(card.render()); document.body.append(section);
  window.cards[sample.id] = card;
  for (const action of card.getAllActions()) {
    if (action.id && action.renderedElement) action.renderedElement.dataset.actionId = action.id;
  }
}
window.ready = true;
`;
const html = `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>Octo card UX benchmark</title><style>${styles}\nbody{margin:24px;background:#eff1f5;color:#20232a;font:14px Arial,'PingFang SC',sans-serif}section{margin-bottom:24px;width:fit-content}h2{font-size:12px;color:#646975;font-weight:400}.bubble{background:white;border-radius:10px;overflow:visible}*{box-sizing:border-box;animation:none!important;transition:none!important}</style><body><script>${renderer}</script><script>${markdown}</script><script>${script}</script></body></html>`;
await writeFile(join(out, "index.html"), html);
await writeFile(join(out, "cards.json"), JSON.stringify(samples, null, 2));
const stats = samples.map(sample => {
  const envelope = { type: 17, card_version: "1.5", ...sample.frame };
  const value = { id: sample.id, ...cardStats(envelope), bytes: Buffer.byteLength(JSON.stringify(envelope)) };
  assert(value.nodes <= 200 && value.maxDepth <= 16 && value.bytes <= 28672, `Card budget exceeded: ${sample.id}`);
  return value;
});
const browser = await chromium.launch({headless:true});
const measurements = [];
let interactions = 0;
let toggleChecks = 0;
const expandedMeasurements = [];
async function measure(section, args) {
  return section.locator('.bubble').evaluate((bubble, args) => {
    const bounds = bubble.getBoundingClientRect();
    const visible = [...bubble.querySelectorAll('*')].filter(el => {
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== 'hidden';
    });
    const overflowing = visible.filter(el => {
      const r = el.getBoundingClientRect();
      return r.right > bounds.right + 1 || r.left < bounds.left - 1 || el.scrollWidth > el.clientWidth + 1;
    }).length;
    const smallActions = visible.filter(el => el.tagName === 'BUTTON' && (el.getBoundingClientRect().height < 28 || el.getBoundingClientRect().width < 28)).length;
    return {...args,height:bounds.height,overflowing,smallActions};
  }, args);
}
try {
  const context = await browser.newContext({viewport:{width:1000,height:1000},deviceScaleFactor:1,locale:"zh-CN",timezoneId:"UTC",reducedMotion:"reduce"});
  await context.route(/^https?:/, route => route.abort());
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  for (const width of [340, 440]) {
    await page.goto(`file://${join(out, "index.html")}?w=${width}`);
    await page.waitForFunction(() => window.ready === true || window.renderErrors?.length > 0);
    await page.evaluate(() => document.fonts.ready);
    assert.deepEqual(errors, []);
    assert.deepEqual(await page.evaluate(() => window.renderErrors), []);
    for (const sample of samples) {
      const section = page.locator(`#${sample.id}`);
      for (const question of sample.questions) assert((await section.innerText()).includes(question.question), `Missing question: ${sample.id}`);
      if (sample.active) {
        for (const question of sample.questions) for (const option of question.options) assert((await section.innerText()).includes(option.label), `Missing option: ${sample.id}`);
      } else assert.equal(await section.locator('input,select,textarea,button').count(), 0, `Terminal controls: ${sample.id}`);
      await section.screenshot({path:join(out, `${sample.id}-${width}.png`)});
      measurements.push(await measure(section, {id:sample.id,width}));
      if (sample.kind === 'progress') {
        const before = await section.innerText();
        let exposedText = before;
        assert(before.trim().length > sample.id.length, `Empty card: ${sample.id}`);
        // Exercise each rendered disclosure in both directions; no synthetic toggle execution.
        const actions = await section.locator('button[data-action-id]:visible').evaluateAll(buttons => buttons.map(button => button.dataset.actionId));
        for (const id of actions) {
          const opposite = id.includes('expand') ? id.replace('expand', 'collapse') : id.replace('collapse', 'expand');
          assert.notEqual(opposite, id, `Unrecognized disclosure: ${id}`);
          await section.locator(`[data-action-id="${id}"]`).click();
          assert.notEqual(await section.innerText(), before, `Disclosure did not change visible content: ${sample.id}/${id}`);
          exposedText += '\n' + await section.innerText();
          assert(await section.locator(`[data-action-id="${opposite}"]`).isVisible(), `Missing return action: ${sample.id}/${id}`);
          expandedMeasurements.push(await measure(section, {id:sample.id,width,action:id}));
          await section.screenshot({path:join(out, `${sample.id}-${width}-${id}.png`)});
          await section.locator(`[data-action-id="${opposite}"]`).click();
          assert.equal(await section.innerText(), before, `Disclosure did not restore content: ${sample.id}/${id}`);
          toggleChecks += 2;
        }
        if (!['starting', 'no-toggle', 'text-only'].includes(sample.id)) assert(actions.length > 0, `Missing progress controls: ${sample.id}`);
        if (['no-toggle', 'text-only'].includes(sample.id)) assert.equal(await section.locator('button').count(), 0);
        if (sample.id !== 'starting') {
          for (const text of ['读取项目配置并确认影响范围', '调整移动端布局并保留完整说明', '验证服务恢复后的交互状态']) {
            assert(exposedText.includes(text), `Unreachable task content: ${sample.id}/${text}`);
          }
          if (sample.id !== 'completed') assert(exposedText.includes('等待测试环境恢复'), `Missing blocker: ${sample.id}`);
        }
        if (sample.id === 'research') {
          for (const text of ['降低卡片渲染延迟', 'render_ms', '建立基线', '减少重复渲染']) {
            assert(exposedText.includes(text), `Unreachable research content: ${text}`);
          }
        }
      }
    }
    // Real renderer clicks -> emitted action/input payload -> production decoder.
    const beforeSelection = await page.evaluate(() => window.submissions.length);
    await page.locator('#single input[type=radio]').nth(1).check();
    assert.equal(await page.evaluate(() => window.submissions.length), beforeSelection);
    await page.locator('#single button').click();
    let event = await page.evaluate(() => window.submissions.at(-1));
    let ref = parseAskActionId(event.id);
    assert(ref);
    assert.deepEqual(resolveAskAnswer(askCardCases[0].questions[0], ref, event.inputs || {}), {kind:'answer',answer:'2'}); interactions++;
    await page.locator('#free input[type=text], #free textarea').fill('仅在工作日发布');
    await page.locator('#free button').click();
    event = await page.evaluate(() => window.submissions.at(-1)); ref = parseAskActionId(event.id); assert(ref);
    assert.deepEqual(resolveAskAnswer(askCardCases[3].questions[0], ref, event.inputs || {}), {kind:'answer',answer:'仅在工作日发布'}); interactions++;
    await page.locator('#multi input[type=checkbox]').nth(0).check();
    await page.locator('#multi input[type=checkbox]').nth(2).check();
    await page.locator('#multi button').click();
    event = await page.evaluate(() => window.submissions.at(-1)); ref = parseAskActionId(event.id); assert(ref);
    assert.deepEqual(resolveAskAnswer(askCardCases[2].questions[0], ref, event.inputs || {}), {kind:'answer',answer:'1,3'}); interactions++;
    const texts = page.locator('#set input[type=text], #set textarea');
    for (let i = 0; i < 3; i++) await texts.nth(i).fill(`补充答案${i+1}`);
    await page.locator('#set button').click();
    event = await page.evaluate(() => window.submissions.at(-1));
    for (let i = 0; i < 3; i++) assert.equal(resolveAskSetAnswer(askCardCases[6].questions[i], i+1, event.inputs || {}), `补充答案${i+1}`);
    interactions++;
    for (const [id, value] of [['mode', 'queue'], ['switch', 'example/fast-response']]) {
      await page.locator(`#${id} select`).selectOption(value);
      await page.locator(`#${id} button`).click();
      const submitted = await page.evaluate(() => window.submissions.at(-1));
      assert.equal(submitted.sample, id);
      assert.equal(submitted.id, `cfg:${id}:fixture`);
      assert.equal(submitted.inputs.config_value, value);
      interactions++;
    }
    assert.deepEqual(errors, [], "No runtime errors after interactions");
  }
} finally { await browser.close(); }
const meanHeight = measurements.reduce((n,m)=>n+m.height,0)/measurements.length;
assert.equal(interactions, 12, "Every viewport must execute six submission paths");
assert(toggleChecks > 0, "Progress disclosures must be exercised");
const allMeasurements = [...measurements, ...expandedMeasurements];
const overflows = allMeasurements.reduce((n,m)=>n+m.overflowing,0);
const smallActions = allMeasurements.reduce((n,m)=>n+m.smallActions,0);
const metrics = {layout_cost: +(meanHeight+1000*overflows+50*smallActions).toFixed(2),mean_height_px:+meanHeight.toFixed(2),overflow_count:overflows,small_action_count:smallActions,max_nodes:Math.max(...stats.map(s=>s.nodes)),max_depth:Math.max(...stats.map(s=>s.maxDepth)),max_payload_bytes:Math.max(...stats.map(s=>s.bytes)),interaction_checks:interactions,toggle_checks:toggleChecks,rendered_cases:measurements.length};
await writeFile(join(out, 'report.json'), JSON.stringify({environment:{renderer:'adaptivecards@3.0.4',chromium:browser.version(),timezone:'UTC',widths:[340,440]},metrics,measurements,expandedMeasurements,stats},null,2));
for (const [name,value] of Object.entries(metrics)) console.log(`METRIC ${name}=${value}`);
console.log(`Visual artifacts: ${out}`);

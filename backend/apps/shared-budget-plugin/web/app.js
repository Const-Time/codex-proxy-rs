'use strict';
const element = id => document.getElementById(id);
const host = window.codexProxyPlugin;
let budget = null;
let reset = { version: null, draft: null };
let busy = false;
const target = () => ({ user_id: element('user').value.trim(), group_id: element('group').value.trim() });
const matches = () => budget && budget.target.user_id === target().user_id && budget.target.group_id === target().group_id;
const pending = () => reset.draft && !reset.draft.receipt && !reset.draft.archived;

function render() {
  document.querySelectorAll('input,button').forEach(node => { node.disabled = busy; });
  element('save-limits').disabled = busy || !matches() || !element('confirm-limits').checked;
  element('prepare-reset').disabled = busy || !matches() || pending();
  element('execute-reset').disabled = busy || !pending() || !element('confirm-reset').checked;
  element('archive-reset').disabled = busy || !reset.draft || reset.draft.archived || !element('confirm-archive').checked;
  element('draft').hidden = !reset.draft;
  if (reset.draft) {
    element('event-id').textContent = reset.draft.command.event_id;
    element('reset-target').textContent = reset.draft.command.targets.map(t => `${t.user_id} / ${t.group_id}`).join(', ');
    element('reset-result').textContent = reset.draft.receipt
      ? `已确认，处理 ${reset.draft.receipt.affected} 个目标`
      : reset.draft.archived ? '已归档；这不代表原操作已取消或未执行' : '尚未确认；准备草稿不会执行重置';
  }
}
async function request(path, data) {
  if (!host) throw new Error('请在网关的插件管理页面中打开此页面。');
  const response = await host.request({path, method:data === undefined ? 'GET' : 'POST', ...(data === undefined ? {} : {contentType:'application/json',body:JSON.stringify(data)})});
  const value = JSON.parse(new TextDecoder().decode(response.body));
  if (response.status < 200 || response.status >= 300) throw new Error(value.error?.message || '操作未完成，请重新读取状态。');
  return value;
}
async function perform(action) {
  if (busy) return;
  busy = true; render(); element('status').textContent = '正在处理…';
  try { await action(); }
  catch (error) { element('status').textContent = error instanceof Error ? error.message : '操作未确认，请读取状态后再决定是否重试。'; }
  finally { busy = false; render(); }
}
const time = value => value == null ? '尚无窗口记录' : `窗口重置时间：${new Date(value).toLocaleString()}`;
async function lookup() {
  budget = await request('api/budget', target());
  element('budget').hidden = false;
  element('daily').textContent = `${budget.daily_used_usd} / ${budget.daily_limit_usd}`;
  element('weekly').textContent = `${budget.weekly_used_usd} / ${budget.weekly_limit_usd}`;
  element('daily-reset').textContent = time(budget.daily_resets_at_ms);
  element('weekly-reset').textContent = time(budget.weekly_resets_at_ms);
  element('multiplier').textContent = `用户额度倍率：${budget.quota_multiplier} · 用户${budget.enabled ? '已启用' : '已停用'}`;
  element('daily-limit').value = budget.group_daily_limit_usd;
  element('weekly-limit').value = budget.group_weekly_limit_usd;
  element('confirm-limits').checked = false;
}
element('lookup').onsubmit = event => { event.preventDefault(); perform(async () => { await lookup(); element('status').textContent = '已读取当前共享额度。'; }); };
for (const id of ['user','group']) element(id).oninput = () => { budget = null; element('budget').hidden = true; element('confirm-limits').checked = false; render(); };
for (const id of ['confirm-limits','confirm-reset','confirm-archive']) element(id).onchange = render;
element('limits').onsubmit = event => {
  event.preventDefault(); if (!matches() || !element('confirm-limits').checked) return;
  perform(async () => {
    const command = {group_id:budget.target.group_id,expected_revision:budget.revision,daily_limit_usd:element('daily-limit').value.trim(),weekly_limit_usd:element('weekly-limit').value.trim()};
    // Never resend automatically. A conflict/unknown result requires another read.
    budget = null;
    await request('api/limits', command);
    await lookup(); element('status').textContent = '分组限额已保存，已重新读取。';
  });
};
element('reload-reset').onclick = () => perform(async () => { reset = await request('api/reset'); element('confirm-reset').checked = false; element('confirm-archive').checked = false; element('status').textContent = '已读取最近的重置操作。'; });
element('prepare-reset').onclick = () => perform(async () => {
  reset = await request('api/reset/prepare', {target:target(),expected_version:reset.version});
  element('confirm-reset').checked = false; element('confirm-archive').checked = false;
  element('status').textContent = '操作编号已保存。请核对目标并确认执行。';
});
element('execute-reset').onclick = () => {
  if (!pending() || !element('confirm-reset').checked) return;
  perform(async () => {
    reset = await request('api/reset/execute', {event_id:reset.draft.command.event_id});
    budget = null; element('budget').hidden = true; element('confirm-reset').checked = false;
    element('status').textContent = '共享用量重置已确认。可重新查询当前用量。';
  });
};
element('archive-reset').onclick = () => {
  if (!reset.draft || !element('confirm-archive').checked) return;
  perform(async () => { reset = await request('api/reset/archive', {event_id:reset.draft.command.event_id,acknowledge_unknown:true}); element('confirm-archive').checked = false; element('status').textContent = '操作草稿已归档，未取消或撤销任何已开始的重置。'; });
};
perform(async () => { reset = await request('api/reset'); element('status').textContent = pending() ? '发现未确认的重置。请核对保存的编号和目标。' : '输入用户与分组 ID 后查询。'; });

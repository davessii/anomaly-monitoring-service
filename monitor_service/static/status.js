// Separate presentation of actual responses and model warnings; no detector changes.
(()=>{
const el=id=>document.getElementById(id);
const names={no_recent_data:'Нет свежих записей запросов',no_response:'Есть запросы без полученного ответа',http_error:'Есть ответы с ошибкой',slow_response:'Есть ответы дольше заданного времени',within_limit:'Последние ответы в заданных пределах',responses_received_no_limit:'Ответы получены; допустимое время не задано'};
const causeNames={no_response:'Ответ не получен',http_error:'Ошибка ответа',slow_response:'Превышено время ответа',unusual_metrics:'Необычные показатели'};
const states={active:'Продолжается',recovering:'Проверяется возврат к обычной работе',resolved:'Возврат подтверждён',observation_ended:'Наблюдение закончено; возврат не подтверждён'};
const when=t=>t==null?'—':new Date(t*1000).toLocaleTimeString('ru-RU');
const ms=v=>v==null?'—':v.toLocaleString('ru-RU',{maximumFractionDigits:1})+' мс';
function episodes(id,rows){
 const body=el(id);body.replaceChildren();
 const items=rows.slice(-30).reverse();
 if(!items.length){const tr=document.createElement('tr'),td=document.createElement('td');td.colSpan=5;td.textContent='Групп уведомлений по текущим правилам нет';tr.append(td);body.append(tr);return;}
 for(const item of items){
  const tr=document.createElement('tr');
  const values=[when(item.started_at),when(item.last_signal_at),item.resolved_at==null?(states[item.status]||item.status):when(item.resolved_at),item.signals,Object.keys(item.causes).map(c=>causeNames[c]||c).join(', ')];
  for(const value of values){const td=document.createElement('td');td.textContent=String(value);tr.append(td);}body.append(tr);
 }
}
function render(status){
 el('service-detail').hidden=!status;if(!status)return;
 el('service-run').textContent='Запуск: '+status.run+(status.observation_ended?' · завершён; показаны сохранённые данные':' · идёт наблюдение');
 el('response-state').textContent=names[status.response_state]||status.response_state;
 el('response-last').textContent=ms(status.last_response_ms);
 el('response-range').textContent=`За последние ${status.recent_window_seconds} с записи: ${status.recent_request_count} завершённых попыток; средняя длительность ${ms(status.recent_mean_ms)}, максимум ${ms(status.recent_max_ms)}. Попытка без ответа тоже учитывается.`;
 el('response-counts').textContent=`Ошибки ответа: ${status.response_counts.http_error}; ответ не получен: ${status.response_counts.no_response}; дольше заданного времени: ${status.policy.latency_limit_ms==null?'проверка не настроена':status.response_counts.slow_response}. При отсутствии свежих записей состояние неизвестно.`;
 el('model-service-state').textContent=status.model_last_decision_time==null?'Решений пока нет':status.model_data_stale?'Решения давно не поступали':`Необычных наблюдений за запуск: ${status.model_signal_observations}. Это предупреждения, а не подтверждённые поломки.`;
 el('service-view-note').textContent=status.view_notice+` Применяемая граница времени: ${status.policy.latency_limit_ms==null?'не задана':ms(status.policy.latency_limit_ms)}. Возврат подтверждается обычными наблюдениями за ${status.policy.recovery_seconds} с. Остановка приложения сама по себе не означает устранение проблемы.`;
 episodes('response-episodes',status.response_incidents);episodes('model-episodes',status.model_incidents);
}
window.ThesisServiceView={render};
})();

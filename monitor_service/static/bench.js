// Separate controls; no quality estimate or automatic threshold adjustment.
(()=>{
let busy=false,viewRun='',policyLoaded=false,policySaving=false;
const el=id=>document.getElementById(id);
const modeName={normal:'Обычная работа',delay:'Добавленная задержка',error:'Созданная ошибка',extra_cpu:'Лишние вычисления'};
async function call(path,data){
  const r=await fetch(path,data===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  const v=await r.json();if(!r.ok)throw Error(v.error||'Не удалось выполнить команду');return v;
}
function message(text){el('bench-message').textContent=text;}
async function refresh(){
  if(busy)return;busy=true;
  try{
    const data=await call('/api/bench');const current=data.current;
    const selector=el('service-run-select');selector.replaceChildren();
    for(const item of data.runs){const option=document.createElement('option');option.value=item.id;option.textContent=item.id+(item.status==='running'?' · работает':' · завершён');selector.append(option);}
    if(!data.runs.some(r=>r.id===viewRun))viewRun=current?.id||data.runs[0]?.id||'';
    selector.value=viewRun;
    if(!policyLoaded&&data.service_policy){el('latency-limit').value=data.service_policy.latency_limit_ms??'';policyLoaded=true;}
    const run=data.runs.find(r=>r.id===current?.id)||data.runs[0];
    const running=!!current?.running,stopping=!!current?.stopping;
    const logMonitor=data.access_log_monitor;
    const logReady=!logMonitor||(logMonitor.ready&&logMonitor.running&&logMonitor.status==='active');
    const ready=!!run?.monitor&&run.monitor.phase==='monitoring'&&logReady;
    const access=current?.access_log_file;
    el('bench-access-status').textContent=access?`Журнал запросов: ${access}. ${running?'В него записываются завершённые запросы. Выберите его выше и нажмите «Новое наблюдение».':'Запуск завершён, файл сохранён; новые строки не поступают.'} ${logMonitor?`Модель журнала: ${logMonitor.samples} из ${logMonitor.required} начальных наблюдений. ${logReady?'':'Проверки сбоев заблокированы до готовности модели и восстановления чтения.'}`:''}`:'Новый запуск приложения автоматически создаст журнал запросов. Старые запуски без него не изменяются.';
    el('bench-start').disabled=running;el('bench-stop').disabled=!running||stopping;
    el('bench-delay').disabled=!running||!ready||stopping;el('bench-errors').disabled=!running||!ready||stopping;
    el('bench-normal').disabled=!running||stopping;
    const rate=current?.target_requests_per_second??2;
    el('bench-extra-cpu').disabled=!running||!ready||stopping||rate!==2;
    el('bench-load-more').disabled=!running||stopping||current?.mode==='extra_cpu';
    el('bench-load-normal').disabled=!running||stopping;
    el('bench-load-state').textContent=running?`Заданный поток: около ${rate} запросов/с; одновременно не более двух. Пропущено запланированных отправок из-за занятых мест: ${current.skipped_slots??0}. ${rate!==2?`Возврат к обычному потоку примерно через ${Math.ceil(current.load_seconds_left??0)} с.`:''}`:'Обычный поток — около двух запросов в секунду; при отдельной проверке задаётся восемь. Фактическое число может быть меньше заданного.';
    if(running){
      const seconds=Math.max(0,Math.round(current.maximum_seconds-(Date.now()/1000-current.started_at)));
      el('bench-status').textContent=stopping?'Приложение завершается; данные сохраняются.':ready?
        `${modeName[current.mode]}. До автоматической остановки — около ${Math.ceil(seconds/60)} мин.`:
        `Начальное обучение приложения: ${run?.monitor?.samples||0} из ${run?.monitor?.warmup_required||896}. Пока работает обычный поток запросов; проверки задержки и ошибок недоступны.`;
      if(!stopping&&logMonitor&&!logReady)el('bench-status').textContent=logMonitor.status!=='active'||!logMonitor.running?'Проверки сбоев недоступны: сначала проверьте чтение журнала и качество данных.':`Начальная настройка модели журнала: ${logMonitor.samples} из ${logMonitor.required}. Пока оставьте обычную работу приложения.`;
    }else{
      el('bench-status').textContent=run?`Последний запуск завершён. ${run.error||'Записи сохранены.'}`:
        'Приложение ещё не запущено. Начальное обучение занимает около 15 минут. Один запуск ограничен 30 минутами.';
    }
    el('bench-detail').hidden=!run;
    const statusRun=viewRun;
    const serviceStatus=statusRun?await call('/api/bench/status?run='+encodeURIComponent(statusRun)):null;
    if(statusRun===viewRun)window.ThesisServiceView.render(serviceStatus);
    if(!run)return;
    el('bench-summary').textContent=`Запросов: ${run.request_count}; ответов с ошибкой или без ответа: ${run.error_responses}; наблюдений модели: ${run.sample_count}; уведомлений модели: ${run.monitor?.notifications??'—'}. ${run.monitor?.delivery_rows===1?'Проверяется каждое измерение; обучение продолжается порциями по 16. Одно уведомление соответствует одному необычному наблюдению.':'Предыдущая версия: решения выдавались порциями по 16 наблюдений.'} Совпадение уведомлений с ухудшением работы нужно проверять отдельно.`;
    const body=el('bench-requests');body.replaceChildren();
    for(const row of run.recent_requests.slice(-12).reverse()){
      const tr=document.createElement('tr');
      const values=[new Date(row.received*1000).toLocaleTimeString('ru-RU'),row.duration_ms.toLocaleString('ru-RU',{maximumFractionDigits:1}),row.status==null?(row.error||'Нет ответа'):String(row.status),modeName[row.served_mode]||'Не установлено',row.extra_cpu_ms==null?'—':row.extra_cpu_ms.toLocaleString('ru-RU',{maximumFractionDigits:2})];
      for(const value of values){const td=document.createElement('td');td.textContent=value;tr.append(td);}body.append(tr);
    }
  }catch(e){message(e.message);window.ThesisServiceView.render(null);}finally{busy=false;}
}
async function action(path,data){
  try{
    message('Команда выполняется…');const result=await call(path,data);if(path.endsWith('/start')){viewRun=result.id;if(result.access_log_file&&window.ThesisLogs)await window.ThesisLogs.selectFile(result.access_log_file);}
    message(path.endsWith('/start')?(result.access_log_file?`Приложение запущено. Выбран журнал ${result.access_log_file}. В блоке журнала нажмите «Новое наблюдение».`:'Приложение запущено. Выберите источник «Учебное приложение» в списке ниже.'):path.endsWith('/stop')?'Остановка запрошена.':path.endsWith('/load')?'Изменён поток запросов только к учебному приложению.':'Команда отправлена только учебному приложению.');await refresh();
  }catch(e){message(e.message);}
}
el('bench-start').onclick=()=>action('/api/bench/start',{});
el('bench-stop').onclick=()=>action('/api/bench/stop',{});
el('bench-delay').onclick=()=>action('/api/bench/control',{mode:'delay',seconds:10});
el('bench-errors').onclick=()=>action('/api/bench/control',{mode:'error',seconds:10});
el('bench-normal').onclick=()=>action('/api/bench/control',{mode:'normal',seconds:10});
el('bench-extra-cpu').onclick=()=>action('/api/bench/control',{mode:'extra_cpu',seconds:10});
el('bench-load-more').onclick=()=>action('/api/bench/load',{rate:8,seconds:10});
el('bench-load-normal').onclick=()=>action('/api/bench/load',{rate:2,seconds:10});
el('service-run-select').onchange=()=>{viewRun=el('service-run-select').value;refresh();};
el('service-policy-save').onclick=async()=>{
 if(policySaving)return;policySaving=true;el('service-policy-save').disabled=true;
 try{
  const raw=el('latency-limit').value.trim();
  if(raw!==''&&(!Number.isFinite(Number(raw))||Number(raw)<=0))throw Error('Введите положительное время в миллисекундах или оставьте поле пустым.');
  await call('/api/bench/settings',{latency_limit_ms:raw===''?null:Number(raw)});
  el('service-message').textContent='Настройка сохранена. Группы показаны по этой границе, исходные записи и IPCA не меняются.';await refresh();
 }catch(e){el('service-message').textContent=e.message;}finally{policySaving=false;el('service-policy-save').disabled=false;}
};
refresh();setInterval(refresh,2000);
})();

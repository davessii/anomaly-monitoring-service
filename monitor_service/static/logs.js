(() => {
  let current = '', loading = false, acting = false, latest = null, preferredFile = '', preferredSource = '';
  function fileProfile(){
    const name=$('log-file').value;
    const app=latest?.application_log;
    const file=latest?.files.find(item=>item.name===name);
    $('log-profile').value=(app?.filename===name?app.profile:null)||file?.profile||'nginx_access_v1';
  }
  function options(id, rows, previous, empty) {
    const control = $(id);
    control.replaceChildren();
    for (const [value, title] of rows) {
      const option = document.createElement('option');
      option.value = value; option.textContent = title; control.append(option);
    }
    if (!rows.length) {
      const option = document.createElement('option');
      option.value = ''; option.textContent = empty; control.append(option);
    }
    if (rows.some(row => row[0] === previous)) control.value = previous;
  }
  function render() {
    if (!latest) return;
    const source = latest.sources.find(item => item.source === current);
    $('log-create').disabled = acting || !$('log-file').value;
    $('log-resume').disabled = acting || !source || source.worker.running || source.status === 'needs_restart';
    $('log-stop').disabled = acting || !source?.worker.running || source.worker.stopping;
    $('log-details').hidden = !source;
    if (!source) return;
    const running = source.worker.running;
    let summary = source.status === 'needs_restart' ? source.reason_text : source.worker.error;
    if (!summary && source.status === 'degraded') summary = source.reason_text;
    if (!summary) summary = running ? (source.worker.stopping ? 'Останавливается после сохранения текущей операции.' : 'Журнал читается. Позиция и результаты сохраняются.') : 'Чтение на паузе. «Продолжить» сохранит прежнюю модель и позицию в файле.';
    text('log-state', summary);
    const message=source.latest_message;
    const precise=source.config?.profile==='owned_access_us_v2';
    const paused=source.status!=='active'||!running;
    const kind=paused?'paused':message?.kind||(source.model?.phase==='unfit'?'unfit':'training');
    const title=paused?'Нет текущих данных':message?.title||(precise?'Модель обучается':'Прежняя модель журнала');
    text('log-message-title',title);
    text('log-message-explanation',paused?summary:message?.message||'Ожидается первая завершённая секунда.');
    $('log-message-state').className='message-state message-'+kind;
    const counts=source.model?.update_counts;
    text('log-learning',counts?`Обновлений модели принято: ${counts.accepted}; отложено из-за качества ответов: ${counts.deferred}.`:'Прежняя схема обработки; новые настройки к этому наблюдению не применялись.');
    table('log-events',source.messages?.length?source.messages.map(m=>[when(m.timestamp),m.title,m.message]):[['Событий пока нет','—','Начальная настройка отображается выше.']]);
    const agg = source.aggregate;
    text('log-requests', agg ? number(agg.accepted_records) : 'Нет данных');
    const malformed = agg ? Object.values(agg.parse_errors).reduce((sum, value) => sum + value, 0) : 0;
    text('log-quality', agg ? `Повреждённых строк: ${malformed}; поздних: ${agg.late_records}.` : 'Сохранённое состояние требует проверки.');
    text('log-samples', source.model ? number(source.model.samples) : 'Нет данных');
    text('log-warmup', source.model?.phase==='unfit'?'Настройка непригодна. Причина указана выше.':source.model?.ready ? 'Начальная настройка завершена.' : `${source.model?.samples ?? 0} из ${source.model?.required ?? 896} начальных наблюдений.`);
    text('log-queue', source.windows.ready || 0);
    text('log-backlog', source.worker.backpressure ? 'Передача отстаёт; чтение временно приостановлено.' : `Не прочитано: ${number(source.reader?.backlog_bytes || 0)} байт.`);
    const window = source.latest_window?.payload;
    table('log-latest', window ? [[when(window.end_ms / 1000), window.request_count,
      window.metrics.mean_request_ms == null ? 'Нет данных' : number(window.metrics.mean_request_ms),
      window.error_5xx_count, window.metrics.server_error_fraction == null ? 'Нет данных' : number(window.metrics.server_error_fraction * 100) + '%',
      window.quality === 'ok' ? 'Пригодны для модели' : window.quality === 'empty' ? 'Нет запросов' : 'Подача в модель приостановлена']] : [['Завершённых интервалов пока нет', '—', '—', '—', '—', '—']]);
    text('log-detail-note', `Файл: ${source.filename}. Создано: ${new Date(source.created).toLocaleString('ru-RU')}. Модель: ${source.host}.`);
  }
  async function update() {
    if (loading) return;
    loading = true;
    try {
      latest = await api('/api/logs');
      text('log-directory', 'Папка доступных журналов: ' + latest.directory);
      const file = preferredFile || $('log-file').value;
      options('log-file', latest.files.map(item => [item.name, item.name+(latest.application_log?.running&&latest.application_log.filename===item.name?' · приложение работает':'')]), file, 'Файлов .log пока нет');
      if(file!==$('log-file').value||preferredFile)fileProfile();
      if ($('log-file').value === preferredFile) preferredFile = '';
      options('log-source', latest.sources.map(item => [item.source, `${item.filename} · ${new Date(item.created).toLocaleString('ru-RU')} · ${item.source.slice(-6)}`]), preferredSource||current, 'Наблюдений пока нет');
      if(preferredSource&&latest.sources.some(item=>item.source===preferredSource)){
        current=preferredSource;preferredSource='';
      }else if(!preferredSource)current=$('log-source').value;
      render();
    } catch (error) {
      text('log-message', error.message); $('log-message').className = 'log-message-error';
    } finally { loading = false; }
  }
  async function action(data, stop) {
    acting = true; render();
    try {
      const response = await api(stop ? '/api/logs/stop' : '/api/logs/start', data);
      current = response.id; preferredSource = response.id;
      selected = response.host;
      text('log-message', stop ? (response.running ? 'Остановка запрошена. Дождитесь завершения записи.' : 'Чтение остановлено. Данные сохранены.') : response.already_running ? 'Это наблюдение уже работает.' : 'Чтение запущено. Ожидается первая полная секунда с запросами.');
      $('log-message').className = '';
      await update(); await refresh();
    } catch (error) {
      text('log-message', error.message); $('log-message').className = 'log-message-error';
    } finally { acting = false; render(); }
  }
  $('log-create').onclick = () => {
    const data={filename:$('log-file').value};
    if($('log-profile').value==='owned_access_us_v2'){
      const latency=$('log-handler-limit').value.trim(), errors=$('log-error-limit').value.trim();
      if(latency===''||errors===''||!Number.isFinite(Number(latency))||Number(latency)<=0||Number(latency)>600000||!Number.isFinite(Number(errors))||Number(errors)<0||Number(errors)>100){
        text('log-message','Задайте время обработки больше нуля и до 600000 мс, а долю ошибок — от 0 до 100%.');
        $('log-message').className='log-message-error';return;
      }
      data.profile='owned_access_us_v2';data.handler_limit_ms=Number(latency);
      data.allowed_error_fraction=Number(errors)/100;
    }
    return action(data,false);
  };
  $('log-resume').onclick = () => action({source_id: current}, false);
  $('log-stop').onclick = () => action({source_id: current}, true);
  $('log-reload').onclick = update;
  $('log-source').onchange = () => { current = $('log-source').value; preferredSource=''; render(); };
  $('log-file').onchange = () => {fileProfile();render();};
  window.ThesisLogs = {selectFile: async filename => { preferredFile = filename; await update(); }};
  update(); setInterval(update, 1000);
})();

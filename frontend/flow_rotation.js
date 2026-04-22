(function () {
  const QUADRANT_COLORS = {
    Leading: '#2b8a3e',
    Weakening: '#f08c00',
    Lagging: '#c92a2a',
    Improving: '#1971c2',
    Neutral: '#6c757d',
  };

  const QUADRANT_SYMBOLS = {
    Leading: 'circle',
    Weakening: 'diamond',
    Lagging: 'x',
    Improving: 'triangle-up',
    Neutral: 'square',
  };

  function fmt(value, digits = 3) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '-';
    }
    return Number(value).toFixed(digits);
  }

  class FlowRotationTab {
    constructor(options) {
      this.getCatalog = options.getCatalog;
      this.root = document.getElementById(options.rootId);
      this.statusEl = document.getElementById(options.statusId);
      this.initialized = false;
      this.currentPayload = null;
    }

    ensureInitialized() {
      if (this.initialized) {
        return;
      }

      this.categoryContainer = document.getElementById('flow-category-filters');
      this.symbolSelect = document.getElementById('flow-symbol-select');
      this.useSectorRelative = document.getElementById('flow-use-sector-relative');
      this.useZScore = document.getElementById('flow-use-zscore');
      this.emaPeriod = document.getElementById('flow-ema-period');
      this.emaValue = document.getElementById('flow-ema-value');
      this.momentumLag = document.getElementById('flow-momentum-lag');
      this.momentumValue = document.getElementById('flow-momentum-value');
      this.tailLength = document.getElementById('flow-tail-length');
      this.tailValue = document.getElementById('flow-tail-value');
      this.tailModeButtons = Array.from(document.querySelectorAll('#flow-tail-mode-toggle button[data-tail-mode]'));
      this.noiseThreshold = document.getElementById('flow-noise-threshold');
      this.applyBtn = document.getElementById('flow-apply-filters');
      this.refreshBtn = document.getElementById('flow-refresh-data');
      this.timeframeButtons = Array.from(document.querySelectorAll('#flow-timeframe-toggle button[data-timeframe]'));
      this.rrgChartId = 'flow-rrg-chart';
      this.sectorChartId = 'flow-sector-chart';
      this.symbolHistoryChartId = 'flow-symbol-history-chart';
      this.cotChartId = 'flow-cot-chart';
      this.priceChartId = 'flow-price-chart';
      this.heatmapBody = document.getElementById('flow-heatmap-body');
      this.sectorOverview = document.getElementById('flow-sector-overview');
      this.topInflow = document.getElementById('flow-top-inflow');
      this.topOutflow = document.getElementById('flow-top-outflow');
      this.metaInfo = document.getElementById('flow-meta-info');

      this.state = {
        selectedCategories: new Set(),
        timeframe: 'medium',
        selectedMarketKey: null,
        selectedCategoryFocus: null,
        tailMode: 'dots',
      };

      this.emaPeriod.addEventListener('input', () => {
        this.emaValue.textContent = this.emaPeriod.value;
      });

      this.momentumLag.addEventListener('input', () => {
        this.momentumValue.textContent = this.momentumLag.value;
      });

      this.tailLength.addEventListener('input', () => {
        this.tailValue.textContent = this.tailLength.value;
      });

      this.tailModeButtons.forEach((button) => {
        button.addEventListener('click', () => {
          this.state.tailMode = button.dataset.tailMode;
          this.tailModeButtons.forEach((item) => item.classList.toggle('active', item === button));
          if (this.currentPayload) {
            this.render(this.currentPayload);
          }
        });
      });

      this.applyBtn.addEventListener('click', () => {
        this.loadFlowRotation(false);
      });

      this.refreshBtn.addEventListener('click', () => {
        this.loadFlowRotation(true);
      });

      this.timeframeButtons.forEach((button) => {
        button.addEventListener('click', () => {
          this.state.timeframe = button.dataset.timeframe;
          this.timeframeButtons.forEach((item) => item.classList.toggle('active', item === button));
          const isCustom = this.state.timeframe === 'custom';
          this.emaPeriod.disabled = !isCustom;
          this.loadFlowRotation(false);
        });
      });

      this.symbolSelect.addEventListener('change', () => {
        this.loadFlowRotation(false);
      });

      this.initialized = true;
      this.bootstrapFilters();
    }

    bootstrapFilters() {
      const catalog = this.getCatalog() || [];
      if (!catalog.length) {
        return;
      }

      const availableCategories = catalog.map((item) => item.name);
      if (!this.state.selectedCategories.size) {
        availableCategories.forEach((item) => this.state.selectedCategories.add(item));
      }

      this.renderCategoryFilters(catalog);
      this.renderSymbolOptions(catalog);
    }

    renderCategoryFilters(catalog) {
      this.categoryContainer.innerHTML = '';
      catalog.forEach((category) => {
        const id = `flow-category-${category.name.replace(/[^a-zA-Z0-9]+/g, '-')}`;
        const checked = this.state.selectedCategories.has(category.name) ? 'checked' : '';
        const row = document.createElement('label');
        row.className = 'flow-check-row';
        row.innerHTML = `
          <input type="checkbox" id="${id}" value="${category.name}" ${checked}>
          <span>${category.name} <small>(${category.markets.length})</small></span>
        `;
        const input = row.querySelector('input');
        input.addEventListener('change', () => {
          if (input.checked) {
            this.state.selectedCategories.add(category.name);
          } else {
            this.state.selectedCategories.delete(category.name);
          }
          this.renderSymbolOptions(catalog);
          this.loadFlowRotation(false);
        });
        this.categoryContainer.appendChild(row);
      });
    }

    renderSymbolOptions(catalog) {
      const selected = catalog.filter((item) => this.state.selectedCategories.has(item.name));
      const existingSelection = new Set(Array.from(this.symbolSelect.selectedOptions).map((option) => option.value));
      this.symbolSelect.innerHTML = '';

      selected.forEach((category) => {
        category.markets.forEach((market) => {
          const option = document.createElement('option');
          option.value = market.symbol;
          option.textContent = `${market.symbol} | ${category.name}`;
          if (!existingSelection.size || existingSelection.has(market.symbol)) {
            option.selected = true;
          }
          this.symbolSelect.appendChild(option);
        });
      });
    }

    getSelectedSymbols() {
      return Array.from(this.symbolSelect.selectedOptions).map((item) => item.value);
    }

    getConfig(forceRefresh) {
      const topN = 5;
      return {
        categories: Array.from(this.state.selectedCategories),
        symbols: this.getSelectedSymbols(),
        useSectorRelative: this.useSectorRelative.checked,
        useZScore: this.useZScore.checked,
        emaPeriod: Number(this.emaPeriod.value),
        momentumLag: Number(this.momentumLag.value),
        tailLength: Number(this.tailLength.value),
        noiseThreshold: Number(this.noiseThreshold.value || 0),
        timeframe: this.state.timeframe,
        tailMode: this.state.tailMode,
        topN,
        forceRefresh,
      };
    }

    async loadFlowRotation(forceRefresh) {
      this.setStatus('Computing flow rotation...');
      const config = this.getConfig(forceRefresh);
      try {
        const response = await fetch('/api/flow-rotation', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(config),
        });
        if (!response.ok) {
          const detail = await response.text();
          throw new Error(`Flow rotation API failed (${response.status}): ${detail || 'no detail'}`);
        }
        let payload;
        try {
          payload = await response.json();
        } catch (parseError) {
          throw new Error(`Flow rotation JSON parse failed: ${parseError?.message || parseError}`);
        }
        this.currentPayload = payload;
        this.render(payload);
        this.setStatus(`Flow Rotation updated at ${new Date(payload.updated_at).toLocaleString()}`);
      } catch (error) {
        console.error(error);
        this.setStatus(`Failed to compute flow rotation: ${error?.message || error}`);
      }
    }

    setStatus(text) {
      this.statusEl.textContent = text;
    }

    render(payload) {
      const points = payload.rrg_points || [];
      const sectorPoints = payload.sector_rrg_points || [];
      const rrgDataset = sectorPoints.length ? sectorPoints : points;

      if (!points.length) {
        this.state.selectedMarketKey = null;
        this.state.selectedCategoryFocus = null;
      } else if (!this.state.selectedMarketKey || !points.some((item) => item.marketKey === this.state.selectedMarketKey)) {
        this.state.selectedMarketKey = points[0].marketKey;
      }

      const selectedSymbolPoint = points.find((item) => item.marketKey === this.state.selectedMarketKey) || null;
      this.state.selectedCategoryFocus = selectedSymbolPoint ? selectedSymbolPoint.category : null;

      this.renderRRG(rrgDataset, this.state.selectedCategoryFocus);
      this.renderHeatmap(payload.heatmap || []);
      this.renderSectorOverview(payload.sector_overview || []);
      this.renderRanking(payload.ranking || { top_inflow: [], top_outflow: [] });
      this.renderSelectedHistory(this.state.selectedMarketKey);
      this.metaInfo.textContent = `Symbols: ${payload.meta?.symbols || 0} | Rows used: ${payload.meta?.rows || 0}`;
    }

    renderRRG(points, selectedCategoryFocus) {
      const useTailLine = this.state.tailMode === 'line';
      const tailTraces = [];
      points.forEach((point) => {
        const tail = point.tail || [];
        if (!tail.length) {
          return;
        }
        tailTraces.push({
          x: tail.slice(0, -1).map((item) => item.rs),
          y: tail.slice(0, -1).map((item) => item.rsMomentum),
          type: 'scatter',
          mode: useTailLine ? 'lines+markers' : 'markers',
          marker: { size: 4, color: point.color, opacity: 0.28 },
          line: useTailLine ? { color: point.color, width: 1.2 } : { width: 0 },
          hoverinfo: 'skip',
          showlegend: false,
        });
      });

      const selectedPoint = points.find((item) => item.category === selectedCategoryFocus);
      const selectedTrace = selectedPoint && (selectedPoint.tail || []).length
        ? [{
          x: selectedPoint.tail.map((item) => item.rs),
          y: selectedPoint.tail.map((item) => item.rsMomentum),
          type: 'scatter',
          mode: useTailLine ? 'lines+markers+text' : 'markers+text',
          text: selectedPoint.tail.map((item, index, arr) => index === arr.length - 1 ? selectedPoint.category : ''),
          textposition: 'top center',
          textfont: { size: 12, color: '#0f172a' },
          marker: { size: 7, color: selectedPoint.color, line: { width: 1, color: '#0f172a' } },
          line: useTailLine ? { color: selectedPoint.color, width: 2.4 } : { width: 0 },
          name: `${selectedPoint.category} trail`,
          hovertemplate: `${selectedPoint.category}<br>RS: %{x:.3f}<br>RS Mom: %{y:.3f}<extra></extra>`,
          showlegend: false,
        }]
        : [];

      const quadrantTraces = ['Leading', 'Weakening', 'Lagging', 'Improving', 'Neutral'].map((quadrant) => {
        const group = points.filter((item) => item.quadrant === quadrant);
        return {
          x: group.map((item) => item.rs),
          y: group.map((item) => item.rsMomentum),
          text: group.map((item) => `${item.category}<br>Flow: ${fmt(item.flow)}<br>Momentum: ${fmt(item.momentum)}<br>RS: ${fmt(item.rs)}<br>RS Mom: ${fmt(item.rsMomentum)}`),
          customdata: group.map((item) => item.category),
          type: 'scatter',
          mode: 'markers+text',
          name: quadrant,
          textposition: 'top center',
          textfont: { size: 11 },
          text: group.map((item) => item.category),
          marker: {
            size: group.map((item) => item.category === selectedCategoryFocus ? 12 : 9),
            color: QUADRANT_COLORS[quadrant],
            symbol: QUADRANT_SYMBOLS[quadrant],
            line: { width: 0.8, color: '#1f2937' },
          },
          hovertemplate: '%{customdata}<br>RS: %{x:.3f}<br>RS Mom: %{y:.3f}<extra></extra>',
        };
      }).filter((trace) => trace.x.length > 0);

      const traces = [...tailTraces, ...selectedTrace, ...quadrantTraces];
      if (!traces.length) {
        document.getElementById(this.rrgChartId).innerHTML = '<div class="empty flow-empty">No data after filters/noise threshold.</div>';
        return;
      }

      Plotly.newPlot(this.rrgChartId, traces, {
        margin: { l: 56, r: 24, t: 28, b: 52 },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#ffffff',
        xaxis: {
          title: 'Relative Strength (RS)',
          showgrid: true,
          gridcolor: '#e8edf3',
          zeroline: false,
        },
        yaxis: {
          title: 'RS Momentum',
          showgrid: true,
          gridcolor: '#e8edf3',
          zeroline: false,
        },
        shapes: [
          {
            type: 'line',
            x0: 1,
            x1: 1,
            y0: Math.min(...points.map((item) => item.rsMomentum), -0.1),
            y1: Math.max(...points.map((item) => item.rsMomentum), 0.1),
            line: { color: '#adb5bd', width: 1, dash: 'dash' },
          },
          {
            type: 'line',
            x0: Math.min(...points.map((item) => item.rs), 0.7),
            x1: Math.max(...points.map((item) => item.rs), 1.3),
            y0: 0,
            y1: 0,
            line: { color: '#adb5bd', width: 1, dash: 'dash' },
          },
        ],
        legend: { orientation: 'h', x: 0, y: 1.1 },
        hovermode: 'closest',
      }, { responsive: true, displaylogo: false });
    }

    renderHeatmap(rows) {
      this.heatmapBody.innerHTML = '';
      const sortedRows = [...rows].sort((a, b) => {
        const f = Number(b.flow || 0) - Number(a.flow || 0);
        if (f !== 0) return f;
        const m = Number(b.momentum || 0) - Number(a.momentum || 0);
        if (m !== 0) return m;
        return Number(b.rs || 0) - Number(a.rs || 0);
      });
      sortedRows.forEach((row) => {
        const tr = document.createElement('tr');
        tr.classList.toggle('active', row.marketKey === this.state.selectedMarketKey);
        const color = QUADRANT_COLORS[row.quadrant] || QUADRANT_COLORS.Neutral;
        tr.innerHTML = `
          <td>${row.symbol}</td>
          <td>${row.category}</td>
          <td>${fmt(row.flow)}</td>
          <td>${fmt(row.momentum)}</td>
          <td>${fmt(row.rs)}</td>
          <td><span class="quad-badge" style="background:${color}">${row.quadrant}</span></td>
        `;
        tr.addEventListener('click', () => {
          this.state.selectedMarketKey = row.marketKey;
          this.render(this.currentPayload || { rrg_points: [], sector_rrg_points: [], heatmap: [], sector_overview: [], ranking: { top_inflow: [], top_outflow: [] }, meta: {} });
        });
        this.heatmapBody.appendChild(tr);
      });
    }

    renderSectorOverview(sectors) {
      this.sectorOverview.innerHTML = '';
      if (!sectors.length) {
        this.sectorOverview.innerHTML = '<div class="flow-empty">No sector overview available.</div>';
        return;
      }

      const strongest = sectors[0]?.category;
      const weakest = sectors[sectors.length - 1]?.category;

      sectors.forEach((sector) => {
        const card = document.createElement('div');
        card.className = 'flow-sector-card';
        const tag = sector.category === strongest ? 'Strongest Inflow' : sector.category === weakest ? 'Weakest' : '';
        card.innerHTML = `
          <h4>${sector.category}</h4>
          <p>Avg Flow: ${fmt(sector.avgFlow)}</p>
          <p>Avg Momentum: ${fmt(sector.avgMomentum)}</p>
          <p>Symbols: ${sector.symbols}</p>
          ${tag ? `<span class="sector-tag">${tag}</span>` : ''}
        `;
        this.sectorOverview.appendChild(card);
      });

      this.renderSectorChart(sectors);
    }

    renderSectorChart(sectors) {
      if (!sectors.length) {
        document.getElementById(this.sectorChartId).innerHTML = '<div class="flow-empty">No sector chart data.</div>';
        return;
      }
      Plotly.newPlot(this.sectorChartId, [
        {
          x: sectors.map((item) => item.category),
          y: sectors.map((item) => item.avgFlow),
          type: 'bar',
          name: 'Avg Flow',
          marker: { color: '#1f77b4' },
        },
        {
          x: sectors.map((item) => item.category),
          y: sectors.map((item) => item.avgMomentum),
          type: 'bar',
          name: 'Avg Momentum',
          marker: { color: '#ff7f0e' },
        },
      ], {
        margin: { l: 42, r: 10, t: 18, b: 70 },
        barmode: 'group',
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#ffffff',
        legend: { orientation: 'h', y: 1.18, x: 0 },
        xaxis: { tickangle: -20 },
        yaxis: { zeroline: true, zerolinecolor: '#d0d7de', gridcolor: '#eef2f7' },
      }, { responsive: true, displaylogo: false });
    }

    renderSelectedHistory(marketKey) {
      const histories = this.currentPayload?.symbol_histories || {};
      const history = histories[marketKey] || [];
      const emptyMsg = '<div class="flow-empty">Select a symbol in heatmap to view history.</div>';
      if (!history.length) {
        document.getElementById(this.symbolHistoryChartId).innerHTML = emptyMsg;
        document.getElementById(this.cotChartId).innerHTML = emptyMsg;
        document.getElementById(this.priceChartId).innerHTML = emptyMsg;
        return;
      }

      const dates = history.map((item) => item.date);

      // Chart 1: Flow / Momentum / RS
      Plotly.newPlot(this.symbolHistoryChartId, [
        {
          x: dates,
          y: history.map((item) => item.flow),
          type: 'scatter', mode: 'lines', name: 'Flow',
          line: { color: '#2b8a3e', width: 2 }, yaxis: 'y',
        },
        {
          x: dates,
          y: history.map((item) => item.momentum),
          type: 'scatter', mode: 'lines', name: 'Momentum',
          line: { color: '#d9480f', width: 1.8 }, yaxis: 'y',
        },
        {
          x: dates,
          y: history.map((item) => item.rs),
          type: 'scatter', mode: 'lines', name: 'RS',
          line: { color: '#1971c2', width: 2 }, yaxis: 'y2',
        },
      ], {
        margin: { l: 52, r: 48, t: 24, b: 40 },
        paper_bgcolor: '#ffffff', plot_bgcolor: '#ffffff',
        hovermode: 'x unified',
        title: { text: 'Flow / Momentum / RS', font: { size: 12 }, x: 0.01 },
        xaxis: { type: 'date', gridcolor: '#eef2f7' },
        yaxis: { title: 'Flow / Mom', gridcolor: '#eef2f7', zeroline: true, zerolinecolor: '#d0d7de' },
        yaxis2: { title: 'RS', overlaying: 'y', side: 'right', showgrid: false },
        legend: { orientation: 'h', y: 1.18, x: 0 },
      }, { responsive: true, displaylogo: false });

      // Chart 2: Long / Short / Net + OI
      Plotly.newPlot(this.cotChartId, [
        {
          x: dates, y: history.map((item) => item.long),
          type: 'scatter', mode: 'lines', name: 'Long',
          line: { color: '#2b8a3e', width: 1.8 }, yaxis: 'y',
        },
        {
          x: dates, y: history.map((item) => item.short),
          type: 'scatter', mode: 'lines', name: 'Short',
          line: { color: '#c92a2a', width: 1.8 }, yaxis: 'y',
        },
        {
          x: dates, y: history.map((item) => item.net),
          type: 'scatter', mode: 'lines', name: 'Net',
          line: { color: '#f08c00', width: 2, dash: 'dot' }, yaxis: 'y',
        },
        {
          x: dates, y: history.map((item) => item.oi),
          type: 'scatter', mode: 'lines', name: 'Open Interest',
          line: { color: '#868e96', width: 1.5 }, yaxis: 'y2',
        },
      ], {
        margin: { l: 60, r: 64, t: 24, b: 40 },
        paper_bgcolor: '#ffffff', plot_bgcolor: '#ffffff',
        hovermode: 'x unified',
        title: { text: 'Long / Short / Net  |  Open Interest', font: { size: 12 }, x: 0.01 },
        xaxis: { type: 'date', gridcolor: '#eef2f7' },
        yaxis: { title: 'Contracts', gridcolor: '#eef2f7', zeroline: true, zerolinecolor: '#d0d7de' },
        yaxis2: { title: 'Open Interest', overlaying: 'y', side: 'right', showgrid: false },
        legend: { orientation: 'h', y: 1.18, x: 0 },
      }, { responsive: true, displaylogo: false });

      // Chart 3: Price (fetched async)
      const priceEl = document.getElementById(this.priceChartId);
      priceEl.innerHTML = '<div class="flow-empty" style="min-height:80px">Loading price...</div>';
      fetch(`/api/series/${encodeURIComponent(marketKey)}`)
        .then((r) => r.ok ? r.json() : null)
        .then((data) => {
          if (!data) { priceEl.innerHTML = '<div class="flow-empty">Price not available.</div>'; return; }
          const ps = data.price_series || [];
          if (!ps.length) { priceEl.innerHTML = '<div class="flow-empty">No price data cached for this symbol.</div>'; return; }
          Plotly.newPlot(this.priceChartId, [
            {
              x: ps.map((p) => p.date), y: ps.map((p) => p.close),
              type: 'scatter', mode: 'lines', name: 'Price',
              line: { color: '#5c7cfa', width: 2 },
            },
          ], {
            margin: { l: 52, r: 16, t: 24, b: 40 },
            paper_bgcolor: '#ffffff', plot_bgcolor: '#ffffff',
            hovermode: 'x unified',
            title: { text: 'Price', font: { size: 12 }, x: 0.01 },
            xaxis: { type: 'date', gridcolor: '#eef2f7' },
            yaxis: { gridcolor: '#eef2f7' },
            showlegend: false,
          }, { responsive: true, displaylogo: false });
        })
        .catch(() => { priceEl.innerHTML = '<div class="flow-empty">Price fetch error.</div>'; });
    }

    renderRanking(ranking) {
      this.topInflow.innerHTML = '';
      this.topOutflow.innerHTML = '';

      (ranking.top_inflow || []).forEach((item) => {
        const li = document.createElement('li');
        li.textContent = `${item.symbol} (${item.category})  M=${fmt(item.momentum)}`;
        this.topInflow.appendChild(li);
      });

      (ranking.top_outflow || []).forEach((item) => {
        const li = document.createElement('li');
        li.textContent = `${item.symbol} (${item.category})  M=${fmt(item.momentum)}`;
        this.topOutflow.appendChild(li);
      });
    }
  }

  window.FlowRotationTab = FlowRotationTab;
})();

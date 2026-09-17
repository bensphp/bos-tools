/**
 * GCP Pub/Sub Explorer (MQTT Explorer Style)
 * Pure vanilla ES6 Frontend - Zero external dependencies
 */

(function () {
  'use strict';

  // State
  const state = {
    isConnected: false,
    isPaused: false,
    projectId: '',
    subscriptionId: '',
    ackMode: 'peek',
    tree: { children: [] },
    selectedNodePath: '',
    selectedNode: null,
    selectedMessage: null,
    historyMessages: [],
    pointHistory: [],
    payloadFormat: 'json',
    searchQuery: '',
    expandedPaths: new Set(),
    activeTab: 'tabPayload',
    theme: localStorage.getItem('theme') || 'dark',
  };

  // Elements
  const el = {
    statusBeacon: document.getElementById('statusBeacon'),
    connectionInfo: document.getElementById('connectionInfo'),
    currentSubDisplay: document.getElementById('currentSubDisplay'),
    infoConfigureBadge: document.getElementById('infoConfigureBadge'),
    statRate: document.getElementById('statRate'),
    statTotal: document.getElementById('statTotal'),
    statThroughput: document.getElementById('statThroughput'),
    statDevices: document.getElementById('statDevices'),
    btnConnect: document.getElementById('btnConnect'),
    btnConnectText: document.getElementById('btnConnectText'),
    btnPause: document.getElementById('btnPause'),
    iconPause: document.getElementById('iconPause'),
    iconPlay: document.getElementById('iconPlay'),
    btnClear: document.getElementById('btnClear'),
    btnSeek: document.getElementById('btnSeek'),
    btnPublish: document.getElementById('btnPublish'),
    btnExport: document.getElementById('btnExport'),
    btnThemeToggle: document.getElementById('btnThemeToggle'),
    treeSearchInput: document.getElementById('treeSearchInput'),
    clearSearchBtn: document.getElementById('clearSearchBtn'),
    btnExpandAll: document.getElementById('btnExpandAll'),
    btnCollapseAll: document.getElementById('btnCollapseAll'),
    selectHierarchyMode: document.getElementById('selectHierarchyMode'),
    treeContainer: document.getElementById('treeContainer'),
    btnEmptyConnect: document.getElementById('btnEmptyConnect'),
    inspectorBreadcrumb: document.getElementById('inspectorBreadcrumb'),
    metaMsgCount: document.getElementById('metaMsgCount'),
    metaLastSeen: document.getElementById('metaLastSeen'),
    badgePointsCount: document.getElementById('badgePointsCount'),
    payloadViewer: document.getElementById('payloadViewer'),
    payloadFilterInput: document.getElementById('payloadFilterInput'),
    btnCopyPayload: document.getElementById('btnCopyPayload'),
    pointsTableBody: document.getElementById('pointsTableBody'),
    pointsFilterInput: document.getElementById('pointsFilterInput'),
    pointsStatsSummary: document.getElementById('pointsStatsSummary'),
    attributesTableBody: document.getElementById('attributesTableBody'),
    metaMsgId: document.getElementById('metaMsgId'),
    metaPublishTime: document.getElementById('metaPublishTime'),
    metaPayloadSize: document.getElementById('metaPayloadSize'),
    metaOrderingKey: document.getElementById('metaOrderingKey'),
    metaDeliveryAttempt: document.getElementById('metaDeliveryAttempt'),
    historyListContainer: document.getElementById('historyListContainer'),
    historyCount: document.getElementById('historyCount'),
    chartTitle: document.getElementById('chartTitle'),
    chartSubtitle: document.getElementById('chartSubtitle'),
    chartMin: document.getElementById('chartMin'),
    chartMax: document.getElementById('chartMax'),
    chartCurrent: document.getElementById('chartCurrent'),
    historyCanvas: document.getElementById('historyCanvas'),
    // Modals
    modalConnect: document.getElementById('modalConnect'),
    presetChipsContainer: document.getElementById('presetChipsContainer'),
    inputProjectId: document.getElementById('inputProjectId'),
    inputSubscriptionId: document.getElementById('inputSubscriptionId'),
    selectAckMode: document.getElementById('selectAckMode'),
    checkSeekOnConnect: document.getElementById('checkSeekOnConnect'),
    btnSubmitConnect: document.getElementById('btnSubmitConnect'),
    btnDisconnectNow: document.getElementById('btnDisconnectNow'),
    connectErrorAlert: document.getElementById('connectErrorAlert'),
    modalSeek: document.getElementById('modalSeek'),
    btnSubmitSeek: document.getElementById('btnSubmitSeek'),
    seekStatusAlert: document.getElementById('seekStatusAlert'),
    modalPublish: document.getElementById('modalPublish'),
    inputPublishTopic: document.getElementById('inputPublishTopic'),
    publishAttributesEditor: document.getElementById('publishAttributesEditor'),
    btnAddAttrRow: document.getElementById('btnAddAttrRow'),
    inputPublishPayload: document.getElementById('inputPublishPayload'),
    btnFormatPublishJson: document.getElementById('btnFormatPublishJson'),
    btnSubmitPublish: document.getElementById('btnSubmitPublish'),
    publishStatusAlert: document.getElementById('publishStatusAlert'),
  };

  // Init Theme
  document.documentElement.setAttribute('data-theme', state.theme);

  // SVG Icons Helper
  const icons = {
    folder: '<svg class="tree-type-icon subfolder" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>',
    device: '<svg class="tree-type-icon device" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>',
    point: '<svg class="tree-type-icon point" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    chevron: '<svg class="tree-toggle-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>',
  };

  // Connect SSE
  let eventSource = null;

  function initSSE() {
    if (eventSource) {
      eventSource.close();
    }
    eventSource = new EventSource('/api/stream');

    eventSource.onmessage = function (e) {
      if (!e.data) return;
      try {
        const payload = JSON.parse(e.data);
        handleServerEvent(payload);
      } catch (err) {
        console.error('Error parsing SSE event:', err);
      }
    };

    eventSource.onerror = function () {
      console.warn('SSE connection lost. Retrying...');
    };
  }

  function handleServerEvent(data) {
    if (data.type === 'init') {
      updateStatus(data.status);
      state.tree = data.tree;
      renderTree();
    } else if (data.type === 'message') {
      if (data.stats) updateStats(data.stats);

      // Flash updated paths in tree
      if (data.updatedPaths && data.updatedPaths.length > 0) {
        flashTreeNodes(data.updatedPaths);
      }

      // Re-fetch tree structure periodically or fetch updated tree
      debouncedRefreshTree();

      // If active node matches incoming message, refresh inspector
      if (state.selectedNodePath && data.message) {
        if (matchesCurrentSelection(data.message)) {
          state.selectedMessage = data.message;
          renderInspector();
          if (state.activeTab === 'tabHistory') {
            loadHistoryForSelection();
          }
        }
      }
    } else if (data.type === 'connect') {
      updateStatus(data.status);
      debouncedRefreshTree();
    } else if (data.type === 'disconnect') {
      updateStatus(data.status);
    } else if (data.type === 'clear') {
      state.tree = { children: [] };
      state.selectedNode = null;
      state.selectedMessage = null;
      renderTree();
      renderInspector();
    }
  }

  let treeRefreshTimeout = null;
  function debouncedRefreshTree() {
    if (treeRefreshTimeout) return;
    treeRefreshTimeout = setTimeout(async () => {
      treeRefreshTimeout = null;
      try {
        const res = await fetch('/api/tree');
        if (res.ok) {
          state.tree = await res.json();
          renderTree();
        }
      } catch (err) {
        console.error('Failed to refresh tree:', err);
      }
    }, 400);
  }

  function openConnectModal() {
    const savedProject = localStorage.getItem('gcp_project') || 'bos-platform-staging';
    const savedSub = localStorage.getItem('gcp_sub') || 'uk-lon-glab-subscription';
    const savedAck = localStorage.getItem('gcp_ack_mode') || 'matched';
    const savedSeek = localStorage.getItem('gcp_seek') !== 'false';

    if (el.inputProjectId) el.inputProjectId.value = state.projectId || savedProject;
    if (el.inputSubscriptionId) el.inputSubscriptionId.value = state.subscriptionId || savedSub;
    if (el.selectAckMode) el.selectAckMode.value = state.ackMode || savedAck;
    if (el.checkSeekOnConnect) el.checkSeekOnConnect.checked = savedSeek;

    if (el.connectErrorAlert) el.connectErrorAlert.classList.add('hidden');
    if (el.modalConnect) el.modalConnect.classList.remove('hidden');

    setTimeout(() => {
      if (el.inputProjectId && !el.inputProjectId.value) {
        el.inputProjectId.focus();
      } else if (el.inputSubscriptionId) {
        el.inputSubscriptionId.focus();
      }
    }, 60);
  }

  function updateStatus(status) {
    state.isConnected = status.isConnected;
    state.isPaused = status.isPaused;
    state.projectId = status.projectId;
    state.subscriptionId = status.subscriptionId;
    state.ackMode = status.ackMode;

    el.statusBeacon.className = 'status-indicator ' + (
      state.isConnected ? (state.isPaused ? 'paused' : 'connected') : 'disconnected'
    );

    if (state.isConnected) {
      el.currentSubDisplay.textContent = `${state.projectId} / ${state.subscriptionId}`;
      el.btnConnectText.textContent = 'Settings';
      if (el.infoConfigureBadge) el.infoConfigureBadge.textContent = 'Change';
      el.btnDisconnectNow.classList.remove('hidden');
    } else {
      el.currentSubDisplay.textContent = 'Not Connected';
      el.btnConnectText.textContent = 'Connect';
      if (el.infoConfigureBadge) el.infoConfigureBadge.textContent = 'Configure';
      el.btnDisconnectNow.classList.add('hidden');
    }

    if (state.isPaused) {
      el.iconPause.classList.add('hidden');
      el.iconPlay.classList.remove('hidden');
      el.btnPause.title = 'Resume Streaming';
    } else {
      el.iconPause.classList.remove('hidden');
      el.iconPlay.classList.add('hidden');
      el.btnPause.title = 'Pause Streaming';
    }

    if (status.stats) updateStats(status.stats);
  }

  function updateStats(stats) {
    el.statRate.innerHTML = `${stats.msg_rate.toFixed(1)} <small>msg/s</small>`;
    el.statTotal.textContent = stats.total_messages.toLocaleString();
    el.statThroughput.innerHTML = `${stats.kb_rate.toFixed(1)} <small>KB/s</small>`;
    el.statDevices.textContent = stats.devices_count.toLocaleString();
  }

  // Flash tree node on update
  function flashTreeNodes(paths) {
    for (const p of paths) {
      const nodeRow = document.querySelector(`.tree-node-row[data-path="${CSS.escape(p)}"]`);
      if (nodeRow) {
        nodeRow.classList.remove('pulse');
        // Trigger reflow to restart animation
        void nodeRow.offsetWidth;
        nodeRow.classList.add('pulse');
      }
    }
  }

  // Tree Rendering
  function renderTree() {
    if (!state.tree || !state.tree.children || state.tree.children.length === 0) {
      el.treeContainer.innerHTML = `
        <div class="empty-state">
          <svg class="empty-icon" viewBox="0 0 24 24"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
          <p>No messages received yet.</p>
          <small>Connect to a subscription to start streaming telemetry.</small>
        </div>
      `;
      return;
    }

    const fragment = document.createDocumentFragment();
    for (const child of state.tree.children) {
      const nodeEl = buildTreeNodeElement(child, 0);
      if (nodeEl) fragment.appendChild(nodeEl);
    }

    el.treeContainer.innerHTML = '';
    el.treeContainer.appendChild(fragment);
  }

  function buildTreeNodeElement(node, depth) {
    const isFiltered = state.searchQuery && !nodeMatchesSearch(node, state.searchQuery.toLowerCase());
    if (isFiltered) return null;

    const wrapper = document.createElement('div');
    wrapper.className = 'tree-node';
    wrapper.dataset.path = node.path;

    // Auto-expand top levels or previously expanded
    const shouldExpand = state.expandedPaths.has(node.path) || (depth < 2 && !state.searchQuery);
    if (shouldExpand) {
      wrapper.classList.add('expanded');
      state.expandedPaths.add(node.path);
    }

    const hasChildren = node.children && node.children.length > 0;

    const row = document.createElement('div');
    row.className = 'tree-node-row' + (state.selectedNodePath === node.path ? ' selected' : '');
    row.dataset.path = node.path;
    row.style.paddingLeft = `${10 + depth * 14}px`;

    // Toggle Icon
    const toggleSpan = document.createElement('span');
    toggleSpan.style.display = 'inline-flex';
    toggleSpan.style.width = '14px';
    if (hasChildren) {
      toggleSpan.innerHTML = icons.chevron;
      toggleSpan.addEventListener('click', (ev) => {
        ev.stopPropagation();
        wrapper.classList.toggle('expanded');
        if (wrapper.classList.contains('expanded')) {
          state.expandedPaths.add(node.path);
        } else {
          state.expandedPaths.delete(node.path);
        }
      });
    }

    // Type icon
    let typeIconHtml = icons.folder;
    if (node.type === 'device') typeIconHtml = icons.device;
    else if (node.type === 'point') typeIconHtml = icons.point;

    const iconSpan = document.createElement('span');
    iconSpan.innerHTML = typeIconHtml;

    // Label
    const labelSpan = document.createElement('span');
    labelSpan.className = 'tree-label';
    labelSpan.textContent = node.label;
    labelSpan.title = node.path;

    // Preview value
    const previewSpan = document.createElement('span');
    previewSpan.className = 'tree-preview-val';
    if (node.lastValue !== null && node.lastValue !== undefined) {
      previewSpan.textContent = typeof node.lastValue === 'object' ? '{...}' : String(node.lastValue);
    }

    // Count badge
    const badgeSpan = document.createElement('span');
    badgeSpan.className = 'tree-badge';
    badgeSpan.textContent = node.count;

    row.appendChild(toggleSpan);
    row.appendChild(iconSpan);
    row.appendChild(labelSpan);
    if (previewSpan.textContent) row.appendChild(previewSpan);
    row.appendChild(badgeSpan);

    // Row selection click
    row.addEventListener('click', () => {
      selectNode(node);
    });

    wrapper.appendChild(row);

    // Children container
    if (hasChildren) {
      const childrenCont = document.createElement('div');
      childrenCont.className = 'tree-children';
      for (const child of node.children) {
        const childEl = buildTreeNodeElement(child, depth + 1);
        if (childEl) childrenCont.appendChild(childEl);
      }
      wrapper.appendChild(childrenCont);
    }

    return wrapper;
  }

  function nodeMatchesSearch(node, query) {
    if (node.label.toLowerCase().includes(query) || node.path.toLowerCase().includes(query)) {
      return true;
    }
    if (node.children) {
      for (const child of node.children) {
        if (nodeMatchesSearch(child, query)) return true;
      }
    }
    return false;
  }

  async function selectNode(node) {
    state.selectedNodePath = node.path;
    state.selectedNode = node;

    // Update row selected class
    document.querySelectorAll('.tree-node-row').forEach((r) => {
      r.classList.toggle('selected', r.dataset.path === node.path);
    });

    // Update breadcrumb
    renderBreadcrumb(node.path);

    // Update node summary
    el.metaMsgCount.textContent = `${node.count} msgs`;
    el.metaLastSeen.textContent = node.lastUpdated ? formatRelativeTime(node.lastUpdated) : '-';

    // Fetch latest message for this node if we have lastMessageId
    if (node.lastMessageId) {
      try {
        const res = await fetch(`/api/messages/${node.lastMessageId}`);
        if (res.ok) {
          state.selectedMessage = await res.json();
        }
      } catch (err) {
        console.error('Failed to load message detail:', err);
      }
    } else {
      // Query recent message
      try {
        const res = await fetch(`/api/messages?filterPath=${encodeURIComponent(node.path)}&limit=1`);
        if (res.ok) {
          const data = await res.json();
          state.selectedMessage = data.messages[0] || null;
        }
      } catch (err) {
        console.error('Failed to query message:', err);
      }
    }

    renderInspector();

    if (state.activeTab === 'tabHistory') {
      loadHistoryForSelection();
    }
  }

  function renderBreadcrumb(path) {
    const parts = path.split('/').filter(Boolean);
    if (parts.length === 0) {
      el.inspectorBreadcrumb.innerHTML = '<span class="crumb-item active">Root</span>';
      return;
    }
    el.inspectorBreadcrumb.innerHTML = parts
      .map((p, idx) => `<span class="crumb-item ${idx === parts.length - 1 ? 'active' : ''}">${escapeHtml(p)}</span>`)
      .join('<span style="color:var(--text-muted)">/</span>');
  }

  function matchesCurrentSelection(msg) {
    if (!state.selectedNodePath) return true;
    const path = state.selectedNodePath;
    const attrs = msg.attributes || {};
    if (path.includes(attrs.deviceId || '')) return true;
    if (path.includes(attrs.gatewayId || '')) return true;
    if (path.includes(attrs.subFolder || '')) return true;
    if (msg.points && Object.keys(msg.points).some((k) => path.endsWith(k))) return true;
    return false;
  }

  // Inspector Tabs Rendering
  function renderInspector() {
    const msg = state.selectedMessage;
    if (!msg) {
      el.payloadViewer.innerHTML = '<div class="table-empty">No message data selected</div>';
      el.pointsTableBody.innerHTML = '<tr><td colspan="5" class="table-empty">No points data</td></tr>';
      el.attributesTableBody.innerHTML = '<tr><td colspan="2" class="table-empty">No attributes</td></tr>';
      el.badgePointsCount.classList.add('hidden');
      return;
    }

    // 1. Render Payload
    renderPayloadView(msg);

    // 2. Render Points Table
    renderPointsTable(msg);

    // 3. Render Attributes & Metadata
    renderAttributes(msg);
  }

  function renderPayloadView(msg) {
    if (state.payloadFormat === 'raw') {
      el.payloadViewer.innerHTML = `<pre style="white-space:pre-wrap;">${escapeHtml(msg.text || '')}</pre>`;
    } else if (state.payloadFormat === 'hex') {
      el.payloadViewer.innerHTML = `<pre>${formatHex(msg.text || '')}</pre>`;
    } else {
      // JSON Tree view
      if (msg.json) {
        el.payloadViewer.innerHTML = '';
        el.payloadViewer.appendChild(buildJsonHtmlTree(msg.json));
      } else {
        el.payloadViewer.innerHTML = `<pre style="white-space:pre-wrap;">${escapeHtml(msg.text || '')}</pre>`;
      }
    }
  }

  function buildJsonHtmlTree(obj, keyName = null) {
    const container = document.createElement('div');
    container.style.paddingLeft = '14px';

    if (obj === null) {
      container.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span class="json-null">null</span>`;
      return container;
    }

    const type = typeof obj;

    if (type === 'string') {
      container.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span class="json-str">"${escapeHtml(obj)}"</span>`;
      return container;
    }

    if (type === 'number') {
      container.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span class="json-num">${obj}</span>`;
      return container;
    }

    if (type === 'boolean') {
      container.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span class="json-bool">${obj}</span>`;
      return container;
    }

    if (Array.isArray(obj)) {
      const header = document.createElement('div');
      header.className = 'json-collapsible';
      header.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span>[ ${obj.length} items ]</span>`;

      const children = document.createElement('div');
      children.className = 'json-children';
      for (let i = 0; i < obj.length; i++) {
        children.appendChild(buildJsonHtmlTree(obj[i], String(i)));
      }

      header.addEventListener('click', () => {
        header.classList.toggle('collapsed');
        children.classList.toggle('collapsed');
      });

      container.appendChild(header);
      container.appendChild(children);
      return container;
    }

    if (type === 'object') {
      const keys = Object.keys(obj);
      const header = document.createElement('div');
      header.className = 'json-collapsible';
      header.innerHTML = (keyName ? `<span class="json-key">"${escapeHtml(keyName)}"</span>: ` : '') +
        `<span>{ ${keys.length} keys }</span>`;

      const children = document.createElement('div');
      children.className = 'json-children';
      for (const k of keys) {
        children.appendChild(buildJsonHtmlTree(obj[k], k));
      }

      header.addEventListener('click', () => {
        header.classList.toggle('collapsed');
        children.classList.toggle('collapsed');
      });

      container.appendChild(header);
      container.appendChild(children);
      return container;
    }

    container.textContent = String(obj);
    return container;
  }

  function renderPointsTable(msg) {
    const points = msg.points || {};
    const keys = Object.keys(points);

    if (keys.length === 0) {
      el.badgePointsCount.classList.add('hidden');
      el.pointsTableBody.innerHTML = '<tr><td colspan="5" class="table-empty">No telemetry points found in message</td></tr>';
      el.pointsStatsSummary.textContent = '';
      return;
    }

    el.badgePointsCount.classList.remove('hidden');
    el.badgePointsCount.textContent = keys.length;
    el.pointsStatsSummary.textContent = `${keys.length} points detected`;

    const filter = (el.pointsFilterInput.value || '').toLowerCase();
    const rows = [];

    for (const ptName of keys.sort()) {
      if (filter && !ptName.toLowerCase().includes(filter)) continue;

      const data = points[ptName];
      let val = '-';
      let units = '';
      let status = '';

      if (typeof data === 'object' && data !== null) {
        val = data.present_value !== undefined ? data.present_value : (data.value !== undefined ? data.value : JSON.stringify(data));
        units = data.units || '';
        status = data.status || (data.alarm ? 'alarm' : '');
      } else {
        val = data;
      }

      let statusHtml = '<span class="status-tag inactive">normal</span>';
      if (status === 'alarm') {
        statusHtml = '<span class="status-tag alarm">ALARM</span>';
      } else if (val === 'active' || val === true) {
        statusHtml = '<span class="status-tag active">ACTIVE</span>';
      } else if (val === 'inactive' || val === false) {
        statusHtml = '<span class="status-tag inactive">INACTIVE</span>';
      }

      const isNumeric = typeof val === 'number';
      const chartBtn = isNumeric
        ? `<button class="btn-sm btn-ghost btn-point-chart" data-point="${escapeHtml(ptName)}">Chart</button>`
        : '';

      rows.push(`
        <tr>
          <td><strong>${escapeHtml(ptName)}</strong></td>
          <td style="color:var(--json-num); font-weight:700;">${escapeHtml(String(val))}</td>
          <td>${escapeHtml(units)}</td>
          <td>${statusHtml}</td>
          <td>${chartBtn}</td>
        </tr>
      `);
    }

    el.pointsTableBody.innerHTML = rows.join('') || '<tr><td colspan="5" class="table-empty">No points match filter</td></tr>';

    // Hook chart buttons
    el.pointsTableBody.querySelectorAll('.btn-point-chart').forEach((btn) => {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const pt = btn.dataset.point;
        switchToHistoryChart(pt);
      });
    });
  }

  function renderAttributes(msg) {
    const attrs = msg.attributes || {};
    const rows = [];
    for (const [k, v] of Object.entries(attrs)) {
      rows.push(`<tr><td><strong>${escapeHtml(k)}</strong></td><td>${escapeHtml(String(v))}</td></tr>`);
    }
    el.attributesTableBody.innerHTML = rows.join('') || '<tr><td colspan="2" class="table-empty">No attributes</td></tr>';

    el.metaMsgId.textContent = msg.id || '-';
    el.metaPublishTime.textContent = msg.publish_time || '-';
    el.metaPayloadSize.textContent = `${msg.size || 0} bytes`;
    el.metaOrderingKey.textContent = msg.ordering_key || '(none)';
    el.metaDeliveryAttempt.textContent = String(msg.delivery_attempt || 0);
  }

  async function loadHistoryForSelection() {
    const path = state.selectedNodePath;
    if (!path) return;

    try {
      const res = await fetch(`/api/messages?filterPath=${encodeURIComponent(path)}&limit=30`);
      if (res.ok) {
        const data = await res.json();
        state.historyMessages = data.messages || [];
        renderHistoryList();
      }
    } catch (err) {
      console.error('Failed to load history:', err);
    }

    // If leaf node is point, load point time-series
    if (state.selectedNode && state.selectedNode.type === 'point') {
      const parts = path.split('/');
      const ptName = parts[parts.length - 1];
      const devId = state.selectedMessage?.deviceId || parts.find((p) => p.includes('-')) || '';
      loadPointHistoryChart(devId, ptName);
    }
  }

  function renderHistoryList() {
    el.historyCount.textContent = `Recent Messages (${state.historyMessages.length})`;
    if (state.historyMessages.length === 0) {
      el.historyListContainer.innerHTML = '<div class="table-empty">No historical messages captured for this node</div>';
      return;
    }

    const items = state.historyMessages.map((m) => {
      const summary = m.deviceId ? `${m.deviceId} [${m.subFolder || 'msg'}]` : m.id;
      return `
        <div class="history-item" data-id="${escapeHtml(m.id)}">
          <div>
            <strong>${escapeHtml(summary)}</strong>
            <div style="font-size:0.75rem; color:var(--text-muted); font-family:var(--font-mono);">${escapeHtml(m.text.substring(0, 70))}...</div>
          </div>
          <span class="history-time">${formatTime(m.received_at)}</span>
        </div>
      `;
    });

    el.historyListContainer.innerHTML = items.join('');

    el.historyListContainer.querySelectorAll('.history-item').forEach((item) => {
      item.addEventListener('click', () => {
        const id = item.dataset.id;
        const msg = state.historyMessages.find((m) => m.id === id);
        if (msg) {
          state.selectedMessage = msg;
          renderInspector();
        }
      });
    });
  }

  async function loadPointHistoryChart(device, point) {
    el.chartTitle.textContent = `${point}`;
    el.chartSubtitle.textContent = `Device: ${device}`;

    try {
      const res = await fetch(`/api/point-history?device=${encodeURIComponent(device)}&point=${encodeURIComponent(point)}`);
      if (res.ok) {
        const data = await res.json();
        state.pointHistory = data.history || [];
        drawHistoryChart(state.pointHistory);
      }
    } catch (err) {
      console.error('Failed to load point chart history:', err);
    }
  }

  function switchToHistoryChart(pointName) {
    const devId = state.selectedMessage?.deviceId || '';
    // Switch to history tab
    document.querySelectorAll('.tab-btn').forEach((b) => b.classList.toggle('active', b.dataset.tab === 'tabHistory'));
    document.querySelectorAll('.tab-pane').forEach((p) => p.classList.toggle('active', p.id === 'tabHistory'));
    state.activeTab = 'tabHistory';
    loadPointHistoryChart(devId, pointName);
  }

  // HTML5 Canvas Time-series Chart Renderer
  function drawHistoryChart(history) {
    const canvas = el.historyCanvas;
    const ctx = canvas.getContext('2d');
    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    if (!history || history.length === 0) {
      ctx.fillStyle = '#64748b';
      ctx.font = '12px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('No numerical history data available for this point yet', width / 2, height / 2);
      el.chartMin.textContent = '-';
      el.chartMax.textContent = '-';
      el.chartCurrent.textContent = '-';
      return;
    }

    const values = history.map((h) => h.v);
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const currVal = values[values.length - 1];

    el.chartMin.textContent = minVal.toFixed(2);
    el.chartMax.textContent = maxVal.toFixed(2);
    el.chartCurrent.textContent = currVal.toFixed(2);

    const padTop = 20;
    const padBottom = 30;
    const padLeft = 45;
    const padRight = 20;
    const plotW = width - padLeft - padRight;
    const plotH = height - padTop - padBottom;

    const valRange = maxVal === minVal ? 1 : maxVal - minVal;

    // Gridlines
    ctx.strokeStyle = '#2b3040';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 0; i <= 4; i++) {
      const y = padTop + (plotH / 4) * i;
      ctx.moveTo(padLeft, y);
      ctx.lineTo(width - padRight, y);

      const valLabel = (maxVal - (valRange / 4) * i).toFixed(1);
      ctx.fillStyle = '#64748b';
      ctx.font = '10px monospace';
      ctx.textAlign = 'right';
      ctx.fillText(valLabel, padLeft - 6, y + 3);
    }
    ctx.stroke();

    // Plot Line
    ctx.strokeStyle = '#3b82f6';
    ctx.lineWidth = 2.5;
    ctx.beginPath();

    const points = history.map((h, i) => {
      const x = padLeft + (plotW / (history.length - 1 || 1)) * i;
      const y = padTop + plotH - ((h.v - minVal) / valRange) * plotH;
      return { x, y, v: h.v, t: h.t };
    });

    points.forEach((pt, i) => {
      if (i === 0) ctx.moveTo(pt.x, pt.y);
      else ctx.lineTo(pt.x, pt.y);
    });
    ctx.stroke();

    // Fill under curve
    if (points.length > 1) {
      const grad = ctx.createLinearGradient(0, padTop, 0, height - padBottom);
      grad.addColorStop(0, 'rgba(59, 130, 246, 0.3)');
      grad.addColorStop(1, 'rgba(59, 130, 246, 0.0)');
      ctx.fillStyle = grad;
      ctx.beginPath();
      ctx.moveTo(points[0].x, points[0].y);
      points.forEach((pt) => ctx.lineTo(pt.x, pt.y));
      ctx.lineTo(points[points.length - 1].x, height - padBottom);
      ctx.lineTo(points[0].x, height - padBottom);
      ctx.closePath();
      ctx.fill();
    }

    // Highlight Dots
    points.forEach((pt) => {
      ctx.fillStyle = '#3b82f6';
      ctx.beginPath();
      ctx.arc(pt.x, pt.y, 3, 0, Math.PI * 2);
      ctx.fill();
    });
  }

  // Format Helpers
  function formatHex(str) {
    let out = '';
    for (let i = 0; i < str.length; i += 16) {
      const chunk = str.slice(i, i + 16);
      const hex = Array.from(chunk)
        .map((c) => c.charCodeAt(0).toString(16).padStart(2, '0'))
        .join(' ');
      const ascii = chunk.replace(/[^\x20-\x7E]/g, '.');
      out += `${i.toString(16).padStart(4, '0')}  ${hex.padEnd(48, ' ')}  |${ascii}|\n`;
    }
    return out;
  }

  function formatTime(timestampSec) {
    if (!timestampSec) return '-';
    const d = new Date(timestampSec * 1000);
    return d.toLocaleTimeString();
  }

  function formatRelativeTime(timestampSec) {
    if (!timestampSec) return '-';
    const diff = Math.floor(Date.now() / 1000 - timestampSec);
    if (diff < 2) return 'just now';
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ago`;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Event Listeners & Actions
  function setupEventListeners() {
    // Top Bar & Empty State Actions
    if (el.btnConnect) {
      el.btnConnect.addEventListener('click', openConnectModal);
    }
    if (el.connectionInfo) {
      el.connectionInfo.addEventListener('click', openConnectModal);
    }
    if (el.btnEmptyConnect) {
      el.btnEmptyConnect.addEventListener('click', openConnectModal);
    }

    // Preset Chips in Connect Modal
    if (el.presetChipsContainer) {
      el.presetChipsContainer.addEventListener('click', (e) => {
        const chip = e.target.closest('.preset-chip');
        if (!chip) return;
        const p = chip.dataset.project;
        const s = chip.dataset.sub;
        if (p && el.inputProjectId) el.inputProjectId.value = p;
        if (s && el.inputSubscriptionId) el.inputSubscriptionId.value = s;
        el.presetChipsContainer.querySelectorAll('.preset-chip').forEach((c) => c.classList.remove('active'));
        chip.classList.add('active');
      });
    }

    // Submit Connect on Enter key in input fields
    [el.inputProjectId, el.inputSubscriptionId].forEach((inp) => {
      if (inp) {
        inp.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            if (el.btnSubmitConnect) el.btnSubmitConnect.click();
          }
        });
      }
    });

    el.btnPause.addEventListener('click', async () => {
      const newState = !state.isPaused;
      try {
        const res = await fetch('/api/pause', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ paused: newState }),
        });
        if (res.ok) {
          state.isPaused = newState;
          updateStatus(state);
        }
      } catch (err) {
        console.error('Failed to toggle pause:', err);
      }
    });

    el.btnClear.addEventListener('click', async () => {
      if (confirm('Clear message buffer and hierarchical tree?')) {
        await fetch('/api/clear', { method: 'POST' });
      }
    });

    el.btnSeek.addEventListener('click', () => {
      el.seekStatusAlert.textContent = '';
      el.seekStatusAlert.className = 'alert alert-info hidden';
      el.modalSeek.classList.remove('hidden');
    });

    el.btnPublish.addEventListener('click', () => {
      el.publishStatusAlert.textContent = '';
      el.publishStatusAlert.className = 'alert alert-info hidden';
      el.modalPublish.classList.remove('hidden');
    });

    el.btnExport.addEventListener('click', () => {
      window.location.href = '/api/export?format=json';
    });

    el.btnThemeToggle.addEventListener('click', () => {
      state.theme = state.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', state.theme);
      localStorage.setItem('theme', state.theme);
    });

    // Tree Toolbar Actions
    el.treeSearchInput.addEventListener('input', (e) => {
      state.searchQuery = e.target.value.trim();
      el.clearSearchBtn.classList.toggle('hidden', !state.searchQuery);
      renderTree();
    });

    el.clearSearchBtn.addEventListener('click', () => {
      el.treeSearchInput.value = '';
      state.searchQuery = '';
      el.clearSearchBtn.classList.add('hidden');
      renderTree();
    });

    el.btnExpandAll.addEventListener('click', () => {
      document.querySelectorAll('.tree-node').forEach((n) => {
        n.classList.add('expanded');
        state.expandedPaths.add(n.dataset.path);
      });
    });

    el.btnCollapseAll.addEventListener('click', () => {
      document.querySelectorAll('.tree-node').forEach((n) => {
        n.classList.remove('expanded');
      });
      state.expandedPaths.clear();
    });

    // Inspector Tabs
    document.querySelectorAll('.tab-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const tabId = btn.dataset.tab;
        state.activeTab = tabId;

        document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach((p) => p.classList.remove('active'));

        btn.classList.add('active');
        document.getElementById(tabId).classList.add('active');

        if (tabId === 'tabHistory') {
          loadHistoryForSelection();
        }
      });
    });

    // Payload Format Toggles
    document.querySelectorAll('.format-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.format-btn').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.payloadFormat = btn.dataset.format;
        if (state.selectedMessage) {
          renderPayloadView(state.selectedMessage);
        }
      });
    });

    el.btnCopyPayload.addEventListener('click', () => {
      if (!state.selectedMessage) return;
      const content = state.selectedMessage.text || '';
      navigator.clipboard.writeText(content).then(() => {
        const orig = el.btnCopyPayload.textContent;
        el.btnCopyPayload.textContent = 'Copied!';
        setTimeout(() => (el.btnCopyPayload.textContent = orig), 1500);
      });
    });

    // Points Table Filter
    el.pointsFilterInput.addEventListener('input', () => {
      if (state.selectedMessage) renderPointsTable(state.selectedMessage);
    });

    // Modal Close buttons
    document.querySelectorAll('[data-close]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const targetId = btn.dataset.close;
        document.getElementById(targetId).classList.add('hidden');
      });
    });

    // Connect Submit
    el.btnSubmitConnect.addEventListener('click', async () => {
      const pId = el.inputProjectId.value.trim();
      const sId = el.inputSubscriptionId.value.trim();
      const ackMode = el.selectAckMode.value;
      const seekNow = el.checkSeekOnConnect ? el.checkSeekOnConnect.checked : true;

      if (!pId || !sId) {
        el.connectErrorAlert.textContent = 'Project ID and Subscription ID are required.';
        el.connectErrorAlert.classList.remove('hidden');
        return;
      }

      el.connectErrorAlert.classList.add('hidden');
      el.btnSubmitConnect.disabled = true;
      el.btnSubmitConnect.innerHTML = '<span class="spinner"></span> Connecting...';

      try {
        const res = await fetch('/api/connect', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ projectId: pId, subscriptionId: sId, ackMode, seekNow }),
        });
        const data = await res.json();
        if (data.success) {
          localStorage.setItem('gcp_project', pId);
          localStorage.setItem('gcp_sub', sId);
          localStorage.setItem('gcp_ack_mode', ackMode);
          localStorage.setItem('gcp_seek', String(seekNow));

          el.modalConnect.classList.add('hidden');
          updateStatus(data.status);
          debouncedRefreshTree();
        } else {
          el.connectErrorAlert.textContent = data.message || 'Connection failed.';
          el.connectErrorAlert.classList.remove('hidden');
        }
      } catch (err) {
        el.connectErrorAlert.textContent = String(err);
        el.connectErrorAlert.classList.remove('hidden');
      } finally {
        el.btnSubmitConnect.disabled = false;
        el.btnSubmitConnect.textContent = 'Connect & Listen';
      }
    });

    el.btnDisconnectNow.addEventListener('click', async () => {
      await fetch('/api/disconnect', { method: 'POST' });
      el.modalConnect.classList.add('hidden');
      state.isConnected = false;
      updateStatus(state);
    });

    // Seek Submit
    el.btnSubmitSeek.addEventListener('click', async () => {
      const opt = document.querySelector('input[name="seekOption"]:checked')?.value || 'now';
      let payload = {};
      if (opt === '15m') payload.relativeMinutes = 15;
      else if (opt === '1h') payload.relativeMinutes = 60;
      else if (opt === '24h') payload.relativeMinutes = 1440;

      el.btnSubmitSeek.disabled = true;
      el.btnSubmitSeek.textContent = 'Seeking...';

      try {
        const res = await fetch('/api/seek', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        el.seekStatusAlert.textContent = data.message;
        el.seekStatusAlert.className = 'alert ' + (data.success ? 'alert-info' : 'alert-error');
        el.seekStatusAlert.classList.remove('hidden');
      } catch (err) {
        el.seekStatusAlert.textContent = String(err);
        el.seekStatusAlert.className = 'alert alert-error';
        el.seekStatusAlert.classList.remove('hidden');
      } finally {
        el.btnSubmitSeek.disabled = false;
        el.btnSubmitSeek.textContent = 'Execute Seek';
      }
    });

    // Publish Attribute Row adder
    el.btnAddAttrRow.addEventListener('click', () => {
      const row = document.createElement('div');
      row.className = 'attr-row';
      row.innerHTML = `
        <input type="text" placeholder="key" class="attr-key">
        <input type="text" placeholder="value" class="attr-val">
      `;
      el.publishAttributesEditor.appendChild(row);
    });

    el.btnFormatPublishJson.addEventListener('click', () => {
      try {
        const parsed = JSON.parse(el.inputPublishPayload.value);
        el.inputPublishPayload.value = JSON.stringify(parsed, null, 2);
      } catch (err) {
        alert('Invalid JSON: ' + err.message);
      }
    });

    // Publish Submit
    el.btnSubmitPublish.addEventListener('click', async () => {
      const topicId = el.inputPublishTopic.value.trim();
      const payloadText = el.inputPublishPayload.value;

      if (!topicId) {
        alert('Topic ID is required.');
        return;
      }

      // Collect attributes
      const attrs = {};
      el.publishAttributesEditor.querySelectorAll('.attr-row').forEach((r) => {
        const k = r.querySelector('.attr-key')?.value.trim();
        const v = r.querySelector('.attr-val')?.value.trim();
        if (k) attrs[k] = v || '';
      });

      el.btnSubmitPublish.disabled = true;
      el.btnSubmitPublish.textContent = 'Publishing...';

      try {
        const res = await fetch('/api/publish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ topicId, data: payloadText, attributes: attrs }),
        });
        const data = await res.json();
        el.publishStatusAlert.textContent = data.message;
        el.publishStatusAlert.className = 'alert ' + (data.success ? 'alert-info' : 'alert-error');
        el.publishStatusAlert.classList.remove('hidden');
      } catch (err) {
        el.publishStatusAlert.textContent = String(err);
        el.publishStatusAlert.className = 'alert alert-error';
        el.publishStatusAlert.classList.remove('hidden');
      } finally {
        el.btnSubmitPublish.disabled = false;
        el.btnSubmitPublish.textContent = 'Publish Message';
      }
    });
  }

  // Bootstrap
  async function init() {
    setupEventListeners();
    initSSE();

    try {
      const res = await fetch('/api/status');
      if (res.ok) {
        const statusData = await res.json();
        updateStatus(statusData);
        if (statusData.isConnected) {
          debouncedRefreshTree();
        } else {
          openConnectModal();
        }
      } else {
        openConnectModal();
      }
    } catch (err) {
      console.warn('Could not fetch initial status:', err);
      openConnectModal();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();


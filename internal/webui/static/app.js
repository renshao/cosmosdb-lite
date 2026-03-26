// CosmosDB Light Explorer - Client Application
(function () {
    'use strict';

    const MASTER_KEY = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==';

    // ── State ──────────────────────────────────────────────────
    let databases = [];
    let selectedDb = null;
    let selectedContainer = null;
    let selectedContainerPartitionKey = null;
    let documents = [];
    let selectedDocId = null;
    let checkedDocIds = new Set();
    let isNewDoc = false;

    // ── DOM refs ───────────────────────────────────────────────
    const $ = (id) => document.getElementById(id);
    const treeView = $('treeView');
    const treeEmpty = $('treeEmpty');
    const welcomePanel = $('welcomePanel');
    const containerView = $('containerView');
    const docTableBody = $('docTableBody');
    const docListEmpty = $('docListEmpty');
    const docCountLabel = $('docCountLabel');
    const docEditorPanel = $('docEditorPanel');
    const jsonEditor = $('jsonEditor');
    const editorTitle = $('editorTitle');
    const loadingOverlay = $('loadingOverlay');
    const toastContainer = $('toastContainer');
    const connectionStatus = $('connectionStatus');
    const selectAllDocs = $('selectAllDocs');

    // ── Auth ───────────────────────────────────────────────────
    async function generateAuthToken(verb, resourceType, resourceLink, date, masterKey) {
        const keyBytes = Uint8Array.from(atob(masterKey), c => c.charCodeAt(0));
        const key = await crypto.subtle.importKey(
            'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
        );
        const text = `${verb.toLowerCase()}\n${resourceType.toLowerCase()}\n${resourceLink}\n${date.toLowerCase()}\n\n`;
        const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(text));
        const sigBase64 = btoa(String.fromCharCode(...new Uint8Array(sig)));
        return encodeURIComponent(`type=master&ver=1.0&sig=${sigBase64}`);
    }

    async function cosmosRequest(method, path, resourceType, resourceLink, body, extraHeaders) {
        const date = new Date().toUTCString();
        const token = await generateAuthToken(method, resourceType, resourceLink, date, MASTER_KEY);
        const headers = {
            'Authorization': decodeURIComponent(token),
            'x-ms-date': date,
            'x-ms-version': '2018-12-31',
            'Content-Type': 'application/json',
            ...extraHeaders
        };
        const opts = { method, headers };
        if (body) opts.body = typeof body === 'string' ? body : JSON.stringify(body);

        const resp = await fetch(path, opts);
        const text = await resp.text();
        let data;
        try { data = JSON.parse(text); } catch { data = text; }

        if (!resp.ok) {
            const msg = (data && data.message) || (data && typeof data === 'object' && JSON.stringify(data)) || text || resp.statusText;
            throw new Error(msg);
        }
        return data;
    }

    // ── Loading / Toasts ───────────────────────────────────────
    let loadingCount = 0;

    function showLoading() {
        loadingCount++;
        loadingOverlay.style.display = 'flex';
    }

    function hideLoading() {
        loadingCount = Math.max(0, loadingCount - 1);
        if (loadingCount === 0) loadingOverlay.style.display = 'none';
    }

    function toast(message, type) {
        const el = document.createElement('div');
        el.className = `toast ${type || 'info'}`;
        el.textContent = message;
        toastContainer.appendChild(el);
        setTimeout(() => {
            el.classList.add('fade-out');
            setTimeout(() => el.remove(), 300);
        }, 3500);
    }

    // ── Database operations ────────────────────────────────────
    async function loadDatabases() {
        showLoading();
        try {
            const data = await cosmosRequest('GET', '/dbs', 'dbs', '', null);
            databases = data.Databases || data.databases || [];
            renderTree();
            connectionStatus.textContent = '● Connected';
            connectionStatus.classList.remove('error');
        } catch (e) {
            toast('Failed to load databases: ' + e.message, 'error');
            connectionStatus.textContent = '● Disconnected';
            connectionStatus.classList.add('error');
        } finally {
            hideLoading();
        }
    }

    async function createDatabase(name) {
        showLoading();
        try {
            await cosmosRequest('POST', '/dbs', 'dbs', '', { id: name });
            toast(`Database "${name}" created`, 'success');
            await loadDatabases();
        } catch (e) {
            toast('Failed to create database: ' + e.message, 'error');
        } finally {
            hideLoading();
        }
    }

    async function deleteDatabase(dbId) {
        showLoading();
        try {
            await cosmosRequest('DELETE', `/dbs/${dbId}`, 'dbs', `dbs/${dbId}`, null);
            toast(`Database "${dbId}" deleted`, 'success');
            if (selectedDb === dbId) {
                selectedDb = null;
                selectedContainer = null;
                showWelcome();
            }
            await loadDatabases();
        } catch (e) {
            toast('Failed to delete database: ' + e.message, 'error');
        } finally {
            hideLoading();
        }
    }

    // ── Container operations ───────────────────────────────────
    async function loadContainers(dbId) {
        showLoading();
        try {
            const data = await cosmosRequest(
                'GET', `/dbs/${dbId}/colls`, 'colls', `dbs/${dbId}`, null
            );
            return data.DocumentCollections || data.documentCollections || [];
        } catch (e) {
            toast('Failed to load containers: ' + e.message, 'error');
            return [];
        } finally {
            hideLoading();
        }
    }

    async function createContainer(dbId, name, partitionKeyPath) {
        showLoading();
        try {
            await cosmosRequest(
                'POST', `/dbs/${dbId}/colls`, 'colls', `dbs/${dbId}`,
                {
                    id: name,
                    partitionKey: { paths: [partitionKeyPath], kind: 'Hash' }
                }
            );
            toast(`Container "${name}" created`, 'success');
            return true;
        } catch (e) {
            toast('Failed to create container: ' + e.message, 'error');
            return false;
        } finally {
            hideLoading();
        }
    }

    async function deleteContainer(dbId, collId) {
        showLoading();
        try {
            await cosmosRequest(
                'DELETE', `/dbs/${dbId}/colls/${collId}`, 'colls', `dbs/${dbId}/colls/${collId}`, null
            );
            toast(`Container "${collId}" deleted`, 'success');
            if (selectedContainer === collId && selectedDb === dbId) {
                selectedContainer = null;
                showWelcome();
            }
            return true;
        } catch (e) {
            toast('Failed to delete container: ' + e.message, 'error');
            return false;
        } finally {
            hideLoading();
        }
    }

    // ── Document operations ────────────────────────────────────
    async function loadDocuments(dbId, collId) {
        showLoading();
        try {
            const data = await cosmosRequest(
                'GET', `/dbs/${dbId}/colls/${collId}/docs`,
                'docs', `dbs/${dbId}/colls/${collId}`, null
            );
            documents = data.Documents || data.documents || [];
            renderDocuments();
        } catch (e) {
            toast('Failed to load documents: ' + e.message, 'error');
            documents = [];
            renderDocuments();
        } finally {
            hideLoading();
        }
    }

    async function queryDocuments(dbId, collId, query) {
        showLoading();
        try {
            const data = await cosmosRequest(
                'POST', `/dbs/${dbId}/colls/${collId}/docs`,
                'docs', `dbs/${dbId}/colls/${collId}`,
                { query: query, parameters: [] },
                {
                    'x-ms-documentdb-isquery': 'True',
                    'Content-Type': 'application/query+json',
                    'x-ms-documentdb-query-enablecrosspartition': 'True'
                }
            );
            documents = data.Documents || data.documents || [];
            renderDocuments();
            toast(`Query returned ${documents.length} document(s)`, 'info');
        } catch (e) {
            toast('Query failed: ' + e.message, 'error');
        } finally {
            hideLoading();
        }
    }

    async function createDocument(dbId, collId, doc) {
        showLoading();
        try {
            const result = await cosmosRequest(
                'POST', `/dbs/${dbId}/colls/${collId}/docs`,
                'docs', `dbs/${dbId}/colls/${collId}`, doc
            );
            toast('Document created', 'success');
            return result;
        } catch (e) {
            toast('Failed to create document: ' + e.message, 'error');
            return null;
        } finally {
            hideLoading();
        }
    }

    async function replaceDocument(dbId, collId, docId, doc) {
        showLoading();
        try {
            const pkValue = getPartitionKeyValue(doc);
            const headers = {};
            if (pkValue !== undefined) {
                headers['x-ms-documentdb-partitionkey'] = JSON.stringify([pkValue]);
            }
            const result = await cosmosRequest(
                'PUT', `/dbs/${dbId}/colls/${collId}/docs/${docId}`,
                'docs', `dbs/${dbId}/colls/${collId}/docs/${docId}`, doc, headers
            );
            toast('Document saved', 'success');
            return result;
        } catch (e) {
            toast('Failed to save document: ' + e.message, 'error');
            return null;
        } finally {
            hideLoading();
        }
    }

    async function deleteDocument(dbId, collId, docId, partitionKeyValue) {
        showLoading();
        try {
            const headers = {};
            if (partitionKeyValue !== undefined) {
                headers['x-ms-documentdb-partitionkey'] = JSON.stringify([partitionKeyValue]);
            }
            await cosmosRequest(
                'DELETE', `/dbs/${dbId}/colls/${collId}/docs/${docId}`,
                'docs', `dbs/${dbId}/colls/${collId}/docs/${docId}`, null, headers
            );
            return true;
        } catch (e) {
            toast('Failed to delete document: ' + e.message, 'error');
            return false;
        } finally {
            hideLoading();
        }
    }

    function getPartitionKeyValue(doc) {
        if (!selectedContainerPartitionKey) return undefined;
        const path = selectedContainerPartitionKey.replace(/^\//, '');
        const parts = path.split('/');
        let val = doc;
        for (const p of parts) {
            if (val == null) return undefined;
            val = val[p];
        }
        return val;
    }

    // ── Tree rendering ─────────────────────────────────────────
    function renderTree() {
        treeEmpty.style.display = databases.length === 0 ? 'block' : 'none';
        // Remove old tree items
        treeView.querySelectorAll('.tree-item').forEach(el => el.remove());

        databases.forEach(db => {
            const item = document.createElement('div');
            item.className = 'tree-item';
            item.dataset.dbId = db.id;

            const node = document.createElement('div');
            node.className = 'tree-node';
            node.innerHTML = `
                <span class="expand-icon">▶</span>
                <span class="node-icon">🗄</span>
                <span class="node-label">${escapeHtml(db.id)}</span>
                <span class="node-actions">
                    <button class="icon-btn add-container-btn" title="Add Container">＋</button>
                    <button class="icon-btn delete-db-btn" title="Delete Database">✕</button>
                </span>
            `;
            item.appendChild(node);

            const children = document.createElement('div');
            children.className = 'tree-children';
            item.appendChild(children);

            // Expand/collapse
            const expandIcon = node.querySelector('.expand-icon');
            node.addEventListener('click', async (e) => {
                if (e.target.closest('.icon-btn')) return;
                const isExpanded = expandIcon.classList.contains('expanded');
                if (isExpanded) {
                    expandIcon.classList.remove('expanded');
                    children.classList.remove('visible');
                } else {
                    expandIcon.classList.add('expanded');
                    children.classList.add('visible');
                    const containers = await loadContainers(db.id);
                    renderContainers(children, db.id, containers);
                }
            });

            // Add container
            node.querySelector('.add-container-btn').addEventListener('click', (e) => {
                e.stopPropagation();
                openCreateContainerDialog(db.id, async () => {
                    expandIcon.classList.add('expanded');
                    children.classList.add('visible');
                    const containers = await loadContainers(db.id);
                    renderContainers(children, db.id, containers);
                });
            });

            // Delete database
            node.querySelector('.delete-db-btn').addEventListener('click', (e) => {
                e.stopPropagation();
                openConfirmDialog(
                    'Delete Database',
                    `Are you sure you want to delete database "${db.id}" and all its containers?`,
                    () => deleteDatabase(db.id)
                );
            });

            treeView.appendChild(item);
        });
    }

    function renderContainers(parentEl, dbId, containers) {
        parentEl.innerHTML = '';
        if (containers.length === 0) {
            parentEl.innerHTML = '<div style="padding: 6px 32px; color: var(--text-dim); font-size: 12px;">No containers</div>';
            return;
        }
        containers.forEach(coll => {
            const node = document.createElement('div');
            node.className = 'tree-node';
            if (selectedDb === dbId && selectedContainer === coll.id) {
                node.classList.add('selected');
            }
            node.innerHTML = `
                <span class="node-icon">📦</span>
                <span class="node-label">${escapeHtml(coll.id)}</span>
                <span class="node-actions">
                    <button class="icon-btn delete-container-btn" title="Delete Container">✕</button>
                </span>
            `;

            node.addEventListener('click', (e) => {
                if (e.target.closest('.icon-btn')) return;
                // Deselect all
                treeView.querySelectorAll('.tree-node.selected').forEach(n => n.classList.remove('selected'));
                node.classList.add('selected');
                selectedDb = dbId;
                selectedContainer = coll.id;
                // Extract partition key path
                if (coll.partitionKey && coll.partitionKey.paths && coll.partitionKey.paths.length > 0) {
                    selectedContainerPartitionKey = coll.partitionKey.paths[0];
                } else {
                    selectedContainerPartitionKey = null;
                }
                openContainer();
            });

            node.querySelector('.delete-container-btn').addEventListener('click', (e) => {
                e.stopPropagation();
                openConfirmDialog(
                    'Delete Container',
                    `Are you sure you want to delete container "${coll.id}" and all its documents?`,
                    async () => {
                        const ok = await deleteContainer(dbId, coll.id);
                        if (ok) {
                            const updated = await loadContainers(dbId);
                            renderContainers(parentEl, dbId, updated);
                        }
                    }
                );
            });

            parentEl.appendChild(node);
        });
    }

    function openContainer() {
        welcomePanel.style.display = 'none';
        containerView.style.display = 'flex';
        closeEditor();
        loadDocuments(selectedDb, selectedContainer);
    }

    function showWelcome() {
        containerView.style.display = 'none';
        welcomePanel.style.display = 'flex';
    }

    // ── Document rendering ─────────────────────────────────────
    function renderDocuments() {
        docTableBody.innerHTML = '';
        checkedDocIds.clear();
        selectAllDocs.checked = false;
        updateDeleteBtn();
        docCountLabel.textContent = `${documents.length} document(s)`;

        if (documents.length === 0) {
            docListEmpty.classList.add('visible');
            return;
        }
        docListEmpty.classList.remove('visible');

        documents.forEach(doc => {
            const tr = document.createElement('tr');
            if (doc.id === selectedDocId) tr.classList.add('selected');

            const pkVal = getPartitionKeyValue(doc);
            const preview = buildPreview(doc);

            tr.innerHTML = `
                <td class="col-check"><input type="checkbox" data-id="${escapeAttr(doc.id)}"></td>
                <td class="col-id" title="${escapeAttr(doc.id)}">${escapeHtml(doc.id)}</td>
                <td class="col-pk" title="${escapeAttr(String(pkVal ?? ''))}">${escapeHtml(String(pkVal ?? ''))}</td>
                <td class="col-preview">${escapeHtml(preview)}</td>
            `;

            const checkbox = tr.querySelector('input[type="checkbox"]');
            checkbox.addEventListener('change', (e) => {
                e.stopPropagation();
                if (checkbox.checked) checkedDocIds.add(doc.id);
                else checkedDocIds.delete(doc.id);
                updateDeleteBtn();
            });

            tr.addEventListener('click', (e) => {
                if (e.target.tagName === 'INPUT') return;
                docTableBody.querySelectorAll('tr.selected').forEach(r => r.classList.remove('selected'));
                tr.classList.add('selected');
                selectedDocId = doc.id;
                openDocEditor(doc);
            });

            docTableBody.appendChild(tr);
        });
    }

    function buildPreview(doc) {
        const skip = new Set(['id', '_rid', '_self', '_etag', '_ts', '_attachments']);
        const parts = [];
        for (const key of Object.keys(doc)) {
            if (skip.has(key)) continue;
            const val = doc[key];
            const str = typeof val === 'object' ? JSON.stringify(val) : String(val);
            parts.push(`${key}: ${str}`);
            if (parts.join(', ').length > 120) break;
        }
        return parts.join(', ');
    }

    function updateDeleteBtn() {
        $('btnDeleteDoc').disabled = checkedDocIds.size === 0;
    }

    // ── Editor ─────────────────────────────────────────────────
    function openDocEditor(doc) {
        isNewDoc = false;
        editorTitle.textContent = `Document — ${doc.id}`;
        jsonEditor.value = JSON.stringify(doc, null, 2);
        docEditorPanel.style.display = 'flex';
    }

    function openNewDocEditor() {
        isNewDoc = true;
        editorTitle.textContent = 'New Document';
        const template = { id: generateId() };
        if (selectedContainerPartitionKey) {
            const key = selectedContainerPartitionKey.replace(/^\//, '');
            template[key] = '';
        }
        jsonEditor.value = JSON.stringify(template, null, 2);
        docEditorPanel.style.display = 'flex';
    }

    function closeEditor() {
        docEditorPanel.style.display = 'none';
        selectedDocId = null;
        isNewDoc = false;
        docTableBody.querySelectorAll('tr.selected').forEach(r => r.classList.remove('selected'));
    }

    async function saveDocument() {
        let doc;
        try {
            doc = JSON.parse(jsonEditor.value);
        } catch (e) {
            toast('Invalid JSON: ' + e.message, 'error');
            return;
        }

        if (!doc.id) {
            toast('Document must have an "id" field', 'error');
            return;
        }

        if (isNewDoc) {
            const result = await createDocument(selectedDb, selectedContainer, doc);
            if (result) {
                isNewDoc = false;
                await loadDocuments(selectedDb, selectedContainer);
                selectedDocId = doc.id;
                openDocEditor(result);
            }
        } else {
            const result = await replaceDocument(selectedDb, selectedContainer, doc.id, doc);
            if (result) {
                await loadDocuments(selectedDb, selectedContainer);
                openDocEditor(result);
            }
        }
    }

    // ── Dialogs ────────────────────────────────────────────────
    function showDialog(id) {
        $(id).style.display = 'flex';
    }

    function hideDialog(id) {
        $(id).style.display = 'none';
    }

    function openCreateContainerDialog(dbId, onCreated) {
        $('newContainerName').value = '';
        $('newPartitionKey').value = '/id';
        showDialog('createContainerDialog');

        const handler = async () => {
            const name = $('newContainerName').value.trim();
            const pk = $('newPartitionKey').value.trim();
            if (!name) { toast('Container name is required', 'error'); return; }
            if (!pk.startsWith('/')) { toast('Partition key must start with /', 'error'); return; }
            hideDialog('createContainerDialog');
            const ok = await createContainer(dbId, name, pk);
            if (ok && onCreated) await onCreated();
        };

        // Replace listener to avoid duplicates
        const btn = $('confirmCreateContainer');
        const newBtn = btn.cloneNode(true);
        btn.parentNode.replaceChild(newBtn, btn);
        newBtn.addEventListener('click', handler);
    }

    let confirmCallback = null;

    function openConfirmDialog(title, message, onConfirm) {
        $('confirmDialogTitle').textContent = title;
        $('confirmDialogMessage').textContent = message;
        confirmCallback = onConfirm;
        showDialog('confirmDialog');
    }

    // ── Helpers ────────────────────────────────────────────────
    function escapeHtml(str) {
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }

    function escapeAttr(str) {
        return str.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function generateId() {
        return crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2, 10);
    }

    // ── Tab key support in editor ──────────────────────────────
    jsonEditor.addEventListener('keydown', (e) => {
        if (e.key === 'Tab') {
            e.preventDefault();
            const start = jsonEditor.selectionStart;
            const end = jsonEditor.selectionEnd;
            jsonEditor.value = jsonEditor.value.substring(0, start) + '  ' + jsonEditor.value.substring(end);
            jsonEditor.selectionStart = jsonEditor.selectionEnd = start + 2;
        }
    });

    // ── Event wiring ───────────────────────────────────────────
    $('btnRefreshDbs').addEventListener('click', loadDatabases);

    $('btnCreateDb').addEventListener('click', () => {
        $('newDbName').value = '';
        showDialog('createDbDialog');
    });

    $('confirmCreateDb').addEventListener('click', () => {
        const name = $('newDbName').value.trim();
        if (!name) { toast('Database name is required', 'error'); return; }
        hideDialog('createDbDialog');
        createDatabase(name);
    });

    $('cancelCreateDb').addEventListener('click', () => hideDialog('createDbDialog'));
    $('closeCreateDb').addEventListener('click', () => hideDialog('createDbDialog'));

    $('cancelCreateContainer').addEventListener('click', () => hideDialog('createContainerDialog'));
    $('closeCreateContainer').addEventListener('click', () => hideDialog('createContainerDialog'));

    $('closeConfirm').addEventListener('click', () => hideDialog('confirmDialog'));
    $('cancelConfirm').addEventListener('click', () => hideDialog('confirmDialog'));
    $('confirmAction').addEventListener('click', () => {
        hideDialog('confirmDialog');
        if (confirmCallback) { confirmCallback(); confirmCallback = null; }
    });

    $('btnNewDoc').addEventListener('click', openNewDocEditor);
    $('btnSaveDoc').addEventListener('click', saveDocument);
    $('btnCloseEditor').addEventListener('click', closeEditor);

    $('btnRefreshDocs').addEventListener('click', () => {
        if (selectedDb && selectedContainer) {
            loadDocuments(selectedDb, selectedContainer);
        }
    });

    $('btnDeleteDoc').addEventListener('click', () => {
        if (checkedDocIds.size === 0) return;
        const count = checkedDocIds.size;
        openConfirmDialog(
            'Delete Documents',
            `Are you sure you want to delete ${count} document(s)?`,
            async () => {
                for (const docId of checkedDocIds) {
                    const doc = documents.find(d => d.id === docId);
                    const pkVal = doc ? getPartitionKeyValue(doc) : undefined;
                    await deleteDocument(selectedDb, selectedContainer, docId, pkVal);
                }
                toast(`${count} document(s) deleted`, 'success');
                checkedDocIds.clear();
                closeEditor();
                await loadDocuments(selectedDb, selectedContainer);
            }
        );
    });

    selectAllDocs.addEventListener('change', () => {
        const checked = selectAllDocs.checked;
        docTableBody.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.checked = checked;
            const id = cb.dataset.id;
            if (checked) checkedDocIds.add(id);
            else checkedDocIds.delete(id);
        });
        updateDeleteBtn();
    });

    $('btnRunQuery').addEventListener('click', () => {
        const q = $('queryInput').value.trim();
        if (!q) { toast('Enter a query', 'error'); return; }
        if (!selectedDb || !selectedContainer) { toast('Select a container first', 'error'); return; }
        queryDocuments(selectedDb, selectedContainer, q);
    });

    $('queryInput').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') $('btnRunQuery').click();
    });

    // Close modals on overlay click
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) overlay.style.display = 'none';
        });
    });

    // Enter key in create-db dialog
    $('newDbName').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') $('confirmCreateDb').click();
    });

    $('newContainerName').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') $('confirmCreateContainer').click();
    });

    // ── Init ───────────────────────────────────────────────────
    loadDatabases();
})();

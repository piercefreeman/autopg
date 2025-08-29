let currentData = {
    heavyScans: [],
    activeQueries: [],
    recommendations: [],
    summary: null
};

// Template utilities
class TemplateRenderer {
    static cloneTemplate(templateId) {
        const template = document.getElementById(templateId);
        if (!template) {
            throw new Error(`Template with id "${templateId}" not found`);
        }
        return template.content.cloneNode(true);
    }

    static populateFields(element, data) {
        const fields = element.querySelectorAll('[data-field]');
        fields.forEach((field) => {
            const fieldName = field.getAttribute('data-field');
            if (fieldName && data.hasOwnProperty(fieldName)) {
                const value = data[fieldName];
                if (field.tagName === 'INPUT' || field.tagName === 'TEXTAREA') {
                    field.value = value;
                } else {
                    field.textContent = value;
                }
            }
        });
    }

    static populateHTML(element, data) {
        const fields = element.querySelectorAll('[data-field]');
        fields.forEach((field) => {
            const fieldName = field.getAttribute('data-field');
            if (fieldName && data.hasOwnProperty(fieldName)) {
                field.innerHTML = data[fieldName];
            }
        });
    }

    static attachEventListeners(element, handlers) {
        const actions = element.querySelectorAll('[data-action]');
        actions.forEach((action) => {
            const actionName = action.getAttribute('data-action');
            if (actionName && handlers[actionName]) {
                action.addEventListener('click', (event) => handlers[actionName](event, action));
            }
        });
    }

    static showAlert(container, message, type = 'success') {
        const fragment = this.cloneTemplate('alert-template');
        const alertElement = fragment.querySelector('.alert');
        if (alertElement) {
            alertElement.classList.add(`alert-${type}`);
            this.populateFields(fragment, { 
                'alert-type': `alert-${type}`,
                'message': message 
            });
        }
        container.innerHTML = '';
        container.appendChild(fragment);
    }

    static showLoading(container, message = 'Loading...') {
        const fragment = this.cloneTemplate('loading-template');
        this.populateFields(fragment, { message });
        container.innerHTML = '';
        container.appendChild(fragment);
    }
}

// API utilities
async function apiCall(endpoint, options = {}) {
    try {
        const response = await fetch(`/api${endpoint}`, options);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('API call failed:', error);
        alert(`Error: ${error.message}`);
        return null;
    }
}

// Main diagnostic functions
async function runFullDiagnostics() {
    showLoading('all');
    
    const summary = await apiCall('/diagnostics/summary');
    if (summary) {
        currentData.summary = summary;
        updateSystemStats(summary);
        displayRecommendations(summary.recommendations);
    }

    await Promise.all([
        getHeavyScans(),
        getActiveQueries(),
        getProblemQueries()
    ]);
}

async function getHeavyScans() {
    const container = document.getElementById('heavy-scans-content');
    TemplateRenderer.showLoading(container);
    
    const data = await apiCall('/diagnostics/heavy-scans?limit=20');
    if (data) {
        currentData.heavyScans = data;
        displayHeavyScans(data, container);
    }
}

async function getActiveQueries() {
    const container = document.getElementById('active-queries-content');
    TemplateRenderer.showLoading(container);
    
    const data = await apiCall('/diagnostics/active-queries?min_duration=1');
    if (data) {
        currentData.activeQueries = data;
        displayActiveQueries(data, container);
    }
}

async function getProblemQueries() {
    const container = document.getElementById('problem-queries-content');
    TemplateRenderer.showLoading(container);
    
    const data = await apiCall('/diagnostics/queries?limit=20');
    if (data) {
        displayProblemQueries(data, container);
    }
}

// Display functions using templates
function displayHeavyScans(tables, container) {
    if (!tables || tables.length === 0) {
        TemplateRenderer.showAlert(container, 'No tables with heavy sequential scans found!', 'success');
        return;
    }

    const tableFragment = TemplateRenderer.cloneTemplate('heavy-scans-table-template');
    const tbody = tableFragment.getElementById('heavy-scans-tbody');

    tables.forEach(table => {
        const rowFragment = TemplateRenderer.cloneTemplate('heavy-scan-row-template');
        const tableName = table.table_name || (table.schemaname && table.relname ? `${table.schemaname}.${table.relname}` : 'Unknown');
        
        const rowData = {
            'table-name': tableName,
            'seq-scan-count': formatNumber(table.seq_scan_count || table.seq_scan || 0),
            'seq-rows-read': formatNumber(table.seq_rows_read || table.seq_tup_read || 0),
            'index-usage': `${table.index_usage_percentage}%`,
            'table-size': table.table_size,
            'severity': table.severity.toUpperCase()
        };

        TemplateRenderer.populateFields(rowFragment, rowData);
        
        // Add severity class
        const severityCell = rowFragment.querySelector('[data-field="severity"]');
        if (severityCell) {
            severityCell.classList.add(`severity-${table.severity}`);
        }

        // Attach event listeners
        TemplateRenderer.attachEventListeners(rowFragment, {
            'analyze-table': () => analyzeTable(tableName)
        });

        tbody.appendChild(rowFragment);
    });

    container.innerHTML = '';
    container.appendChild(tableFragment);
}

function displayActiveQueries(queries, container) {
    if (!queries || queries.length === 0) {
        TemplateRenderer.showAlert(container, 'No long-running queries detected!', 'success');
        return;
    }

    const tableFragment = TemplateRenderer.cloneTemplate('active-queries-table-template');
    const tbody = tableFragment.getElementById('active-queries-tbody');

    queries.forEach(query => {
        const rowFragment = TemplateRenderer.cloneTemplate('active-query-row-template');
        const queryText = query.query.substring(0, 100) + (query.query.length > 100 ? '...' : '');
        
        const rowData = {
            'pid': query.pid.toString(),
            'duration': formatDuration(query.duration_seconds),
            'state': query.state,
            'query': escapeHtml(queryText)
        };

        TemplateRenderer.populateFields(rowFragment, rowData);

        // Attach event listeners
        TemplateRenderer.attachEventListeners(rowFragment, {
            'kill-query': () => killQuery(query.pid)
        });

        tbody.appendChild(rowFragment);
    });

    container.innerHTML = '';
    container.appendChild(tableFragment);
}

function displayProblemQueries(queries, container) {
    if (!queries || queries.length === 0) {
        TemplateRenderer.showAlert(container, 'No problematic queries found!', 'success');
        return;
    }

    const listFragment = TemplateRenderer.cloneTemplate('problem-queries-list-template');
    const listContainer = listFragment.getElementById('problem-queries-container');

    queries.forEach(query => {
        const itemFragment = TemplateRenderer.cloneTemplate('problem-query-item-template');
        
        const itemData = {
            'calls': formatNumber(query.calls),
            'total-time': formatDuration(query.total_time_ms / 1000),
            'avg-time': `${query.mean_time_ms.toFixed(2)}ms`,
            'max-time': `${query.max_time_ms.toFixed(2)}ms`
        };

        TemplateRenderer.populateFields(itemFragment, itemData);

        // Handle HTML content for query text
        const queryTextElement = itemFragment.querySelector('[data-field="query-text"]');
        if (queryTextElement) {
            if (query.query_text_html) {
                queryTextElement.innerHTML = query.query_text_html;
            } else {
                queryTextElement.innerHTML = `<code>${escapeHtml(query.query_text)}</code>`;
            }
        }

        listContainer.appendChild(itemFragment);
    });

    container.innerHTML = '';
    container.appendChild(listFragment);
}

function displayRecommendations(recommendations) {
    const container = document.getElementById('recommendations-content');
    
    if (!recommendations || recommendations.length === 0) {
        TemplateRenderer.showAlert(container, 'No critical issues found!', 'success');
        return;
    }

    const listFragment = TemplateRenderer.cloneTemplate('recommendations-list-template');
    const listContainer = listFragment.getElementById('recommendations-container');

    recommendations.forEach((rec, index) => {
        const itemFragment = TemplateRenderer.cloneTemplate('recommendation-item-template');
        
        const itemData = {
            'title': `${index + 1}. ${rec.table_name}`,
            'reason': rec.reason,
            'improvement': rec.estimated_improvement,
            'create-statement': escapeHtml(rec.create_statement)
        };

        TemplateRenderer.populateFields(itemFragment, itemData);
        listContainer.appendChild(itemFragment);
    });

    container.innerHTML = '';
    container.appendChild(listFragment);
}

// Modal functions
async function analyzeTable(tableName) {
    if (!tableName || tableName === 'undefined' || tableName === 'Unknown') {
        alert('Invalid table name. Please try refreshing the data.');
        return;
    }

    const modal = document.getElementById('tableModal');
    modal.style.display = 'block';
    document.getElementById('modal-title').textContent = `Analysis: ${tableName}`;
    
    const modalBody = document.getElementById('modal-body');
    TemplateRenderer.showLoading(modalBody);

    const data = await apiCall(`/diagnostics/table/${encodeURIComponent(tableName)}`);
    if (data) {
        displayTableAnalysis(data, modalBody);
    }
}

function displayTableAnalysis(analysis, container) {
    container.innerHTML = '';

    // Scan Statistics
    const scanStatsFragment = TemplateRenderer.cloneTemplate('modal-scan-stats-template');
    const scanStatsData = {
        'seq-scan-count': formatNumber(analysis.scan_stats.seq_scan_count),
        'seq-rows-read': formatNumber(analysis.scan_stats.seq_rows_read),
        'index-usage': `${analysis.scan_stats.index_usage_percentage}%`,
        'table-size': analysis.scan_stats.table_size
    };
    TemplateRenderer.populateFields(scanStatsFragment, scanStatsData);
    container.appendChild(scanStatsFragment);

    // Existing Indexes
    const indexesFragment = TemplateRenderer.cloneTemplate('modal-indexes-section-template');
    const indexesContainer = indexesFragment.getElementById('modal-indexes-container');

    if (analysis.indexes && analysis.indexes.length > 0) {
        analysis.indexes.forEach((idx) => {
            const indexFragment = TemplateRenderer.cloneTemplate('modal-index-item-template');
            const indexData = {
                'index-name': idx.index_name,
                'index-size': idx.index_size
            };
            TemplateRenderer.populateFields(indexFragment, indexData);

            // Handle HTML content for index definition
            const indexDefElement = indexFragment.querySelector('[data-field="index-def"]');
            if (indexDefElement) {
                if (idx.index_def_html) {
                    indexDefElement.innerHTML = idx.index_def_html;
                } else {
                    indexDefElement.innerHTML = `<code>${escapeHtml(idx.index_def)}</code>`;
                }
            }

            indexesContainer.appendChild(indexFragment);
        });
    } else {
        TemplateRenderer.showAlert(indexesContainer, 'No indexes found on this table.', 'warning');
    }
    container.appendChild(indexesFragment);

    // Recommendations
    if (analysis.recommendations && analysis.recommendations.length > 0) {
        const recommendationsFragment = TemplateRenderer.cloneTemplate('modal-recommendations-section-template');
        const recommendationsContainer = recommendationsFragment.getElementById('modal-recommendations-container');

        analysis.recommendations.forEach((rec) => {
            const recFragment = TemplateRenderer.cloneTemplate('modal-recommendation-item-template');
            TemplateRenderer.populateFields(recFragment, { 'recommendation-text': rec });
            recommendationsContainer.appendChild(recFragment);
        });

        container.appendChild(recommendationsFragment);
    }

    // Problem Queries
    if (analysis.problem_queries && analysis.problem_queries.length > 0) {
        const problemQueriesFragment = TemplateRenderer.cloneTemplate('modal-problem-queries-section-template');
        const problemQueriesContainer = problemQueriesFragment.getElementById('modal-problem-queries-container');

        analysis.problem_queries.forEach((query) => {
            const queryFragment = TemplateRenderer.cloneTemplate('problem-query-item-template');
            
            const queryData = {
                'calls': formatNumber(query.calls),
                'total-time': formatDuration(query.total_time_ms / 1000),
                'avg-time': `${query.mean_time_ms.toFixed(2)}ms`,
                'max-time': `${query.max_time_ms.toFixed(2)}ms`
            };

            TemplateRenderer.populateFields(queryFragment, queryData);

            // Handle HTML content for query text
            const queryTextElement = queryFragment.querySelector('[data-field="query-text"]');
            if (queryTextElement) {
                if (query.query_text_html) {
                    queryTextElement.innerHTML = query.query_text_html;
                } else {
                    queryTextElement.innerHTML = `<code>${escapeHtml(query.query_text)}</code>`;
                }
            }

            problemQueriesContainer.appendChild(queryFragment);
        });

        container.appendChild(problemQueriesFragment);
    }
}

async function killQuery(pid) {
    if (!confirm(`Are you sure you want to terminate query with PID ${pid}?`)) {
        return;
    }

    const result = await apiCall(`/diagnostics/kill-query/${pid}`, { method: 'POST' });
    if (result && result.success) {
        alert(result.message);
        await getActiveQueries();
    }
}

// System stats update
function updateSystemStats(summary) {
    document.getElementById('total-seq-reads').textContent = formatNumberShort(summary.total_seq_reads);
    document.getElementById('total-idx-reads').textContent = formatNumberShort(summary.total_idx_reads);
    document.getElementById('critical-tables').textContent = summary.critical_tables.length.toString();
    document.getElementById('active-queries').textContent = summary.active_problems.length.toString();
}

// Loading utilities
function showLoading(section) {
    const containers = {
        'heavy-scans': 'heavy-scans-content',
        'active-queries': 'active-queries-content',
        'problem-queries': 'problem-queries-content',
        'recommendations': 'recommendations-content'
    };

    const sectionsToUpdate = section === 'all' ? Object.values(containers) : [containers[section]].filter(Boolean);

    sectionsToUpdate.forEach(containerId => {
        const container = document.getElementById(containerId);
        if (container) {
            TemplateRenderer.showLoading(container);
        }
    });
}

// Modal utilities
function closeModal() {
    document.getElementById('tableModal').style.display = 'none';
}

// Utility functions
function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatNumberShort(num) {
    if (num >= 1e12) return (num / 1e12).toFixed(1) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(1) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    return num.toString();
}

function formatDuration(seconds) {
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
    return `${(seconds / 3600).toFixed(1)}h`;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function refreshData() {
    await runFullDiagnostics();
}

// Event listeners
window.onclick = function(event) {
    const modal = document.getElementById('tableModal');
    if (event.target == modal) {
        modal.style.display = 'none';
    }
}

// Initialize on page load
window.onload = async function() {
    await runFullDiagnostics();
}
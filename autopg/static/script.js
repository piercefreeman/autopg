let currentData = {
    heavyScans: [],
    activeQueries: [],
    recommendations: [],
    summary: null
};

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

async function runFullDiagnostics() {
    showLoading('all');
    
    const summary = await apiCall('/diagnostics/summary');
    if (summary) {
        currentData.summary = summary;
        updateSystemStats(summary);
        displayRecommendations(summary.recommendations);
    }

    await getHeavyScans();
    await getActiveQueries();
    await getProblemQueries();
}

async function getHeavyScans() {
    showLoading('heavy-scans');
    const data = await apiCall('/diagnostics/heavy-scans?limit=20');
    if (data) {
        currentData.heavyScans = data;
        displayHeavyScans(data);
    }
}

async function getActiveQueries() {
    showLoading('active-queries');
    const data = await apiCall('/diagnostics/active-queries?min_duration=1');
    if (data) {
        currentData.activeQueries = data;
        displayActiveQueries(data);
    }
}

async function getProblemQueries() {
    showLoading('problem-queries');
    const data = await apiCall('/diagnostics/queries?limit=20');
    if (data) {
        displayProblemQueries(data);
    }
}

function displayHeavyScans(tables) {
    if (!tables || tables.length === 0) {
        document.getElementById('heavy-scans-content').innerHTML = 
            '<div class="alert alert-success">No tables with heavy sequential scans found!</div>';
        return;
    }

    let html = `
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Table</th>
                        <th>Sequential Scans</th>
                        <th>Seq Rows Read</th>
                        <th>Index Usage %</th>
                        <th>Size</th>
                        <th>Severity</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
    `;

    tables.forEach(table => {
        // Fix undefined table name issue - use the raw field names from the API
        const tableName = table.table_name || (table.schemaname && table.relname ? `${table.schemaname}.${table.relname}` : 'Unknown');
        const severityClass = `severity-${table.severity}`;
        
        html += `
            <tr>
                <td><strong>${escapeHtml(tableName)}</strong></td>
                <td>${formatNumber(table.seq_scan_count || table.seq_scan || 0)}</td>
                <td>${formatNumber(table.seq_rows_read || table.seq_tup_read || 0)}</td>
                <td>${table.index_usage_percentage}%</td>
                <td>${table.table_size}</td>
                <td class="${severityClass}">${table.severity.toUpperCase()}</td>
                <td>
                    <button class="btn" onclick="analyzeTable('${escapeHtml(tableName)}')">
                        Analyze
                    </button>
                </td>
            </tr>
        `;
    });

    html += '</tbody></table></div>';
    document.getElementById('heavy-scans-content').innerHTML = html;
}

function displayActiveQueries(queries) {
    if (!queries || queries.length === 0) {
        document.getElementById('active-queries-content').innerHTML = 
            '<div class="alert alert-success">No long-running queries detected!</div>';
        return;
    }

    let html = `
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>PID</th>
                        <th>Duration</th>
                        <th>State</th>
                        <th>Query</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
    `;

    queries.forEach(query => {
        const duration = formatDuration(query.duration_seconds);
        const queryText = query.query.substring(0, 100) + (query.query.length > 100 ? '...' : '');
        html += `
            <tr>
                <td>${query.pid}</td>
                <td>${duration}</td>
                <td>${query.state}</td>
                <td><code>${escapeHtml(queryText)}</code></td>
                <td>
                    <button class="btn btn-primary" onclick="killQuery(${query.pid})">
                        Kill
                    </button>
                </td>
            </tr>
        `;
    });

    html += '</tbody></table></div>';
    document.getElementById('active-queries-content').innerHTML = html;
}

function displayProblemQueries(queries) {
    if (!queries || queries.length === 0) {
        document.getElementById('problem-queries-content').innerHTML = 
            '<div class="alert alert-success">No problematic queries found!</div>';
        return;
    }

    let html = `
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Calls</th>
                        <th>Total Time</th>
                        <th>Avg Time</th>
                        <th>Max Time</th>
                    </tr>
                </thead>
                <tbody>
    `;

    queries.forEach(query => {
        const queryText = query.query_text.substring(0, 100) + (query.query_text.length > 100 ? '...' : '');
        html += `
            <tr>
                <td><code>${escapeHtml(queryText)}</code></td>
                <td>${formatNumber(query.calls)}</td>
                <td>${formatDuration(query.total_time_ms / 1000)}</td>
                <td>${query.mean_time_ms.toFixed(2)}ms</td>
                <td>${query.max_time_ms.toFixed(2)}ms</td>
            </tr>
        `;
    });

    html += '</tbody></table></div>';
    document.getElementById('problem-queries-content').innerHTML = html;
}

function displayRecommendations(recommendations) {
    if (!recommendations || recommendations.length === 0) {
        document.getElementById('recommendations-content').innerHTML = 
            '<div class="alert alert-success">No critical issues found!</div>';
        return;
    }

    let html = '<div>';
    recommendations.forEach((rec, index) => {
        html += `
            <div class="recommendation">
                <h3>${index + 1}. ${rec.table_name}</h3>
                <p><strong>Reason:</strong> ${rec.reason}</p>
                <p><strong>Expected Improvement:</strong> ${rec.estimated_improvement}</p>
                <div class="code-block">${escapeHtml(rec.create_statement)}</div>
            </div>
        `;
    });
    html += '</div>';

    document.getElementById('recommendations-content').innerHTML = html;
}

async function analyzeTable(tableName) {
    if (!tableName || tableName === 'undefined' || tableName === 'Unknown') {
        alert('Invalid table name. Please try refreshing the data.');
        return;
    }

    const modal = document.getElementById('tableModal');
    modal.style.display = 'block';
    document.getElementById('modal-title').textContent = `Analysis: ${tableName}`;
    document.getElementById('modal-body').innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    const data = await apiCall(`/diagnostics/table/${encodeURIComponent(tableName)}`);
    if (data) {
        displayTableAnalysis(data);
    }
}

function displayTableAnalysis(analysis) {
    let html = `
        <div>
            <h3>Scan Statistics</h3>
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat-value">${formatNumber(analysis.scan_stats.seq_scan_count)}</div>
                    <div class="stat-label">Sequential Scans</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">${formatNumber(analysis.scan_stats.seq_rows_read)}</div>
                    <div class="stat-label">Rows Read</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">${analysis.scan_stats.index_usage_percentage}%</div>
                    <div class="stat-label">Index Usage</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">${analysis.scan_stats.table_size}</div>
                    <div class="stat-label">Table Size</div>
                </div>
            </div>

            <h3>Existing Indexes</h3>
    `;

    if (analysis.indexes && analysis.indexes.length > 0) {
        html += '<ul>';
        analysis.indexes.forEach(idx => {
            html += `<li><strong>${idx.index_name}</strong> (${idx.index_size}): ${escapeHtml(idx.index_def)}</li>`;
        });
        html += '</ul>';
    } else {
        html += '<p>No indexes found on this table.</p>';
    }

    if (analysis.recommendations && analysis.recommendations.length > 0) {
        html += '<h3>Recommendations</h3>';
        analysis.recommendations.forEach(rec => {
            html += `<div class="recommendation">${rec}</div>`;
        });
    }

    if (analysis.problem_queries && analysis.problem_queries.length > 0) {
        html += '<h3>Problem Queries</h3>';
        html += '<ul>';
        analysis.problem_queries.forEach(query => {
            html += `<li>${escapeHtml(query.query_text.substring(0, 100))} - ${query.calls} calls, avg ${query.mean_time_ms}ms</li>`;
        });
        html += '</ul>';
    }

    html += '</div>';
    document.getElementById('modal-body').innerHTML = html;
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

function updateSystemStats(summary) {
    document.getElementById('total-seq-reads').textContent = formatNumberShort(summary.total_seq_reads);
    document.getElementById('total-idx-reads').textContent = formatNumberShort(summary.total_idx_reads);
    document.getElementById('critical-tables').textContent = summary.critical_tables.length;
    document.getElementById('active-queries').textContent = summary.active_problems.length;
}

function showLoading(section) {
    const loadingHtml = '<div class="loading"><div class="spinner"></div>Loading...</div>';
    
    if (section === 'all' || section === 'heavy-scans') {
        document.getElementById('heavy-scans-content').innerHTML = loadingHtml;
    }
    if (section === 'all' || section === 'active-queries') {
        document.getElementById('active-queries-content').innerHTML = loadingHtml;
    }
    if (section === 'all' || section === 'problem-queries') {
        document.getElementById('problem-queries-content').innerHTML = loadingHtml;
    }
    if (section === 'all' || section === 'recommendations') {
        document.getElementById('recommendations-content').innerHTML = loadingHtml;
    }
}

function closeModal() {
    document.getElementById('tableModal').style.display = 'none';
}

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

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('tableModal');
    if (event.target == modal) {
        modal.style.display = 'none';
    }
}

// Load initial data on page load
window.onload = async function() {
    await runFullDiagnostics();
}

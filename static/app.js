document.addEventListener('DOMContentLoaded', () => {
    const nodesContainer = document.getElementById('nodes-container');
    const template = document.getElementById('node-card-template');
    
    // Global UI Elements
    const globalTerm = document.getElementById('global-term');
    const globalLeader = document.getElementById('global-leader');
    const commandInput = document.getElementById('command-input');
    const submitBtn = document.getElementById('submit-btn');

    // State caching for micro-animations
    const nodeStateCache = new Map();

    // Setup command submission
    submitBtn.addEventListener('click', async () => {
        const cmd = commandInput.value.trim();
        if (!cmd) return;
        
        try {
            const res = await fetch('/command', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ command: cmd })
            });
            if (res.ok) {
                commandInput.value = '';
                commandInput.placeholder = 'Command submitted!';
                setTimeout(() => commandInput.placeholder = 'Enter command (e.g. SET x 100)', 2000);
            } else {
                alert('Failed to submit command: ' + await res.text());
            }
        } catch (err) {
            alert('Error: ' + err.message);
        }
    });

    // Handle node kill/restart
    async function performNodeAction(id, action) {
        try {
            const res = await fetch(`/node/${id}/${action}`, { method: 'POST' });
            if (!res.ok) {
                alert(`Failed to ${action} node ${id}`);
            }
            fetchCluster(); // Refresh immediately
        } catch (err) {
            alert(`Error: ${err.message}`);
        }
    }

    function createOrUpdateNodeCard(node) {
        let card = nodesContainer.querySelector(`.node-card[data-id="${node.id}"]`);
        
        if (!card) {
            // Clone template
            const clone = template.content.cloneNode(true);
            card = clone.querySelector('.node-card');
            card.dataset.id = node.id;
            card.querySelector('.node-id').textContent = node.id;
            
            // Attach events
            card.querySelector('.kill-btn').addEventListener('click', () => performNodeAction(node.id, 'kill'));
            card.querySelector('.restart-btn').addEventListener('click', () => performNodeAction(node.id, 'restart'));
            
            nodesContainer.appendChild(card);
            nodeStateCache.set(node.id, {});
        }

        const cache = nodeStateCache.get(node.id);

        // Update state
        if (cache.state !== node.state) {
            card.dataset.state = node.state;
            card.querySelector('.state-badge').textContent = node.state;
            cache.state = node.state;
        }

        // Update kill/restart buttons based on state
        const killBtn = card.querySelector('.kill-btn');
        const restartBtn = card.querySelector('.restart-btn');
        if (node.state === 'DEAD') {
            killBtn.style.display = 'none';
            restartBtn.style.display = 'block';
        } else {
            killBtn.style.display = 'block';
            restartBtn.style.display = 'none';
        }

        if (node.state === 'DEAD') return; // Don't update metrics if dead

        // Helper to update text and trigger highlight if changed
        const updateMetric = (selector, key, newValue) => {
            if (cache[key] !== newValue) {
                const el = card.querySelector(selector);
                el.textContent = newValue;
                if (cache[key] !== undefined) {
                    el.classList.add('highlight');
                    setTimeout(() => el.classList.remove('highlight'), 300);
                }
                cache[key] = newValue;
            }
        };

        updateMetric('.m-term', 'term', node.term);
        updateMetric('.m-commit', 'commit', node.commitIndex);
        updateMetric('.m-log', 'log', node.logLength);
        updateMetric('.m-elections', 'elections', node.ElectionsWon);
        updateMetric('.m-commands', 'commands', `${node.CommandsCommitted || 0} / ${node.CommandsApplied || 0}`);
        updateMetric('.m-rpc', 'rpc', `${node.RPCSent || 0} / ${node.RPCReceived || 0}`);
        updateMetric('.m-heartbeats', 'heartbeats', `${node.HeartbeatsSent || 0} / ${node.HeartbeatsReceived || 0}`);
    }

    async function fetchCluster() {
        try {
            // We fetch both endpoints to combine data. 
            // The prompt says /cluster returns basic info, /metrics returns all metrics.
            const [clusterRes, metricsRes] = await Promise.all([
                fetch('/cluster'),
                fetch('/metrics')
            ]);

            if (!clusterRes.ok || !metricsRes.ok) return;

            const cluster = await clusterRes.json();
            const metrics = await metricsRes.json() || [];

            globalTerm.textContent = cluster.term;
            globalLeader.textContent = cluster.leader > 0 ? `Node ${cluster.leader}` : 'None';

            // Map metrics by node ID
            const metricsMap = new Map();
            metrics.forEach(m => metricsMap.set(m.NodeID, m));

            // Create/update cards
            cluster.nodes.forEach(n => {
                const nodeMetrics = metricsMap.get(n.id) || {};
                createOrUpdateNodeCard({ ...n, ...nodeMetrics });
            });

            // Clean up old nodes if any (e.g. cluster size changed)
            const activeIds = new Set(cluster.nodes.map(n => n.id));
            nodesContainer.querySelectorAll('.node-card').forEach(card => {
                if (!activeIds.has(parseInt(card.dataset.id))) {
                    card.remove();
                }
            });

        } catch (err) {
            console.error('Failed to fetch cluster state:', err);
        }
    }

    // Start polling
    fetchCluster();
    setInterval(fetchCluster, 1000);
});

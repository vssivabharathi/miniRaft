import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Power, PowerOff, Send, Zap } from 'lucide-react';

export default function ChaosControls() {
  const [nodeId, setNodeId] = useState('1');
  const [command, setCommand] = useState('');
  const queryClient = useQueryClient();

  const killMutation = useMutation({
    mutationFn: async (id: string) => fetch(`/node/${id}/kill`, { method: 'POST' }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['cluster'] })
  });

  const restartMutation = useMutation({
    mutationFn: async (id: string) => fetch(`/node/${id}/restart`, { method: 'POST' }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['cluster'] })
  });

  const commandMutation = useMutation({
    mutationFn: async (cmd: string) => {
      const res = await fetch(`/command`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command: cmd })
      });
      if (!res.ok) throw new Error(await res.text());
      return res.json();
    },
    onSuccess: () => {
      setCommand('');
      queryClient.invalidateQueries({ queryKey: ['cluster'] });
      queryClient.invalidateQueries({ queryKey: ['logs'] });
      queryClient.invalidateQueries({ queryKey: ['stateMachine'] });
      queryClient.invalidateQueries({ queryKey: ['metrics'] });
    }
  });

  return (
    <div className="bg-panel rounded-md border border-border-subtle p-5 shadow-sm">
      <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-5 flex items-center gap-2">
        <Zap className="w-4 h-4 text-warning" />
        Chaos Controls
      </h2>
      
      <div className="space-y-5">
        <div>
          <label className="block mb-2 text-[10px] font-bold text-text-muted uppercase tracking-wider">Node Management</label>
          <div className="flex gap-2">
            <select 
              value={nodeId} 
              onChange={e => setNodeId(e.target.value)}
              className="bg-background border border-border-subtle text-text-primary text-sm rounded-md focus:ring-primary focus:border-primary block w-24 p-2 outline-none"
            >
              <option value="1">N 1</option>
              <option value="2">N 2</option>
              <option value="3">N 3</option>
            </select>
            <button 
              onClick={() => killMutation.mutate(nodeId)}
              className="bg-danger/10 hover:bg-danger/20 text-danger px-3 py-2 rounded-md border border-danger/30 transition-colors flex items-center justify-center gap-2 flex-1 text-xs font-semibold"
            >
              <PowerOff className="w-3.5 h-3.5" /> Kill
            </button>
            <button 
              onClick={() => restartMutation.mutate(nodeId)}
              className="bg-success/10 hover:bg-success/20 text-success px-3 py-2 rounded-md border border-success/30 transition-colors flex items-center justify-center gap-2 flex-1 text-xs font-semibold"
            >
              <Power className="w-3.5 h-3.5" /> Restart
            </button>
          </div>
        </div>

        <div className="pt-4 border-t border-border-subtle">
          <label className="block mb-2 text-[10px] font-bold text-text-muted uppercase tracking-wider">Client Request</label>
          <div className="flex gap-2">
            <input 
              type="text" 
              value={command}
              onChange={e => setCommand(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && command.trim() && commandMutation.mutate(command)}
              placeholder="e.g. SET key 100"
              className="bg-background border border-border-subtle text-text-primary text-sm rounded-md focus:ring-primary focus:border-primary block w-full p-2 outline-none font-mono"
            />
            <button 
              onClick={() => commandMutation.mutate(command)}
              disabled={!command.trim() || commandMutation.isPending}
              className="bg-primary hover:bg-primary/90 text-white px-4 py-2 rounded-md transition-colors flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
          {commandMutation.isError && (
            <p className="mt-2 text-xs text-danger bg-danger/10 p-2 rounded border border-danger/20">
              {commandMutation.error.message}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

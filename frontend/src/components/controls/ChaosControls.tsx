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
    }
  });

  return (
    <div className="bg-slate-800 rounded-xl border border-slate-700 p-5 shadow-xl">
      <h2 className="text-lg font-semibold text-slate-200 mb-5 flex items-center gap-2">
        <Zap className="w-5 h-5 text-amber-400" />
        Chaos Controls
      </h2>
      
      <div className="space-y-5">
        <div>
          <label className="block mb-2 text-xs font-medium text-slate-400 uppercase tracking-wider">Node Management</label>
          <div className="flex gap-2">
            <select 
              value={nodeId} 
              onChange={e => setNodeId(e.target.value)}
              className="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-24 p-2.5 outline-none"
            >
              <option value="1">N 1</option>
              <option value="2">N 2</option>
              <option value="3">N 3</option>
            </select>
            <button 
              onClick={() => killMutation.mutate(nodeId)}
              className="bg-red-500/10 hover:bg-red-500/20 text-red-400 px-3 py-2 rounded-lg border border-red-500/30 transition-colors flex items-center justify-center gap-2 flex-1 text-sm font-medium"
            >
              <PowerOff className="w-4 h-4" /> Kill
            </button>
            <button 
              onClick={() => restartMutation.mutate(nodeId)}
              className="bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-400 px-3 py-2 rounded-lg border border-emerald-500/30 transition-colors flex items-center justify-center gap-2 flex-1 text-sm font-medium"
            >
              <Power className="w-4 h-4" /> Restart
            </button>
          </div>
        </div>

        <div className="pt-4 border-t border-slate-700/50">
          <label className="block mb-2 text-xs font-medium text-slate-400 uppercase tracking-wider">Client Request</label>
          <div className="flex gap-2">
            <input 
              type="text" 
              value={command}
              onChange={e => setCommand(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && command.trim() && commandMutation.mutate(command)}
              placeholder="e.g. SET key 100"
              className="bg-slate-900 border border-slate-700 text-slate-200 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 outline-none font-mono"
            />
            <button 
              onClick={() => commandMutation.mutate(command)}
              disabled={!command.trim() || commandMutation.isPending}
              className="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-lg transition-colors flex items-center justify-center disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-blue-900/50"
            >
              <Send className="w-4 h-4" />
            </button>
          </div>
          {commandMutation.isError && (
            <p className="mt-2 text-xs text-red-400 bg-red-900/20 p-2 rounded border border-red-900/50">
              {commandMutation.error.message}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

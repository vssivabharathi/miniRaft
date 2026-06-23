import type { ClusterEvent } from '../../types';
import { useEffect, useRef } from 'react';
import { Info, AlertTriangle, XCircle, CheckCircle2 } from 'lucide-react';

interface Props {
  events: ClusterEvent[];
}

export default function EventTimeline({ events }: Props) {
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [events]);

  return (
    <div className="flex flex-col h-full bg-slate-900">
      <div className="p-4 border-b border-slate-700 bg-slate-800 shrink-0 shadow-sm z-10">
        <h2 className="text-lg font-semibold text-slate-200 flex items-center gap-2">
          <Info className="w-5 h-5 text-blue-400" />
          Event Timeline
        </h2>
      </div>
      <div className="p-4 overflow-y-auto flex-1 space-y-3 custom-scrollbar">
        {events.length === 0 ? (
          <div className="text-center text-slate-500 mt-10 italic">Waiting for cluster events...</div>
        ) : (
          events.map((event, idx) => {
            const isLast = idx === events.length - 1;
            
            let Icon = Info;
            let color = 'text-blue-400';
            let bg = 'bg-blue-500/10';
            
            if (event.type === 'success') { Icon = CheckCircle2; color = 'text-emerald-400'; bg = 'bg-emerald-500/10'; }
            if (event.type === 'warning') { Icon = AlertTriangle; color = 'text-amber-400'; bg = 'bg-amber-500/10'; }
            if (event.type === 'error') { Icon = XCircle; color = 'text-red-400'; bg = 'bg-red-500/10'; }

            return (
              <div 
                key={event.id} 
                className={`flex gap-3 text-sm transition-all duration-500 rounded-lg p-2 ${bg} ${
                  isLast ? 'opacity-100 shadow-md border border-slate-700/50 scale-100' : 'opacity-70 border border-transparent'
                }`}
              >
                <div className={`mt-0.5 shrink-0 ${color}`}>
                  <Icon className="w-4 h-4" />
                </div>
                <div className="flex flex-col w-full">
                  <div className="flex justify-between items-start gap-2">
                    <span className={`text-slate-200 ${isLast ? 'font-medium' : ''}`}>{event.message}</span>
                    <span className="text-xs text-slate-500 font-mono shrink-0">
                      {event.timestamp.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                    </span>
                  </div>
                </div>
              </div>
            );
          })
        )}
        <div ref={endRef} className="h-1" />
      </div>
    </div>
  );
}

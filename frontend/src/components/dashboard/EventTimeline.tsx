import React, { useEffect, useRef } from 'react';
import type { ClusterEvent } from '../../types';
import { Terminal } from 'lucide-react';

interface Props {
  events: ClusterEvent[];
}

const EventTimelineComponent = ({ events }: Props) => {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events]);

  return (
    <div className="flex flex-col h-full bg-panel rounded-md border border-border-subtle shadow-sm">
      <div className="p-4 border-b border-border-subtle flex items-center gap-2 bg-background/50">
        <Terminal className="w-4 h-4 text-text-muted" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">Cluster Event Feed</h2>
      </div>

      <div 
        ref={scrollRef}
        className="flex-1 overflow-y-auto p-4 space-y-2 custom-scrollbar bg-background/20"
      >
        {events.length === 0 ? (
          <div className="text-center text-text-muted text-sm mt-10">No events recorded yet.</div>
        ) : (
          events.map((event) => {
            let colorClass = 'text-text-primary border-border-subtle bg-panel';
            let dotColor = 'bg-text-muted';
            
            if (event.type === 'error') {
              colorClass = 'text-danger border-danger/20 bg-danger/5';
              dotColor = 'bg-danger';
            } else if (event.type === 'success') {
              colorClass = 'text-success border-success/20 bg-success/5';
              dotColor = 'bg-success';
            } else if (event.type === 'warning') {
              colorClass = 'text-warning border-warning/20 bg-warning/5';
              dotColor = 'bg-warning';
            } else if (event.type === 'info') {
              colorClass = 'text-primary border-primary/20 bg-primary/5';
              dotColor = 'bg-primary';
            }

            return (
              <div 
                key={event.id}
                className={`text-xs p-2 rounded border flex items-start gap-3 transition-colors ${colorClass}`}
              >
                <div className="flex flex-col items-center gap-1 mt-1">
                  <div className={`w-2 h-2 rounded-full ${dotColor}`} />
                </div>
                <div className="flex-1 font-mono">
                  <span className="opacity-60 mr-2">[{event.timestamp.toLocaleTimeString()}]</span>
                  <span className="font-medium">{event.message}</span>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};

export default React.memo(EventTimelineComponent);

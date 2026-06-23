import LogReplicationPanel from '../components/dashboard/LogReplicationPanel';

export default function LogsPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6">
      <LogReplicationPanel 
        logs={data.logs} 
        isLoading={data.isLoading?.logs} 
      />
    </div>
  );
}

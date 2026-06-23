import SnapshotPanel from '../components/dashboard/SnapshotPanel';

export default function SnapshotsPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6">
      <SnapshotPanel 
        snapshots={data.snapshots} 
        snapshotStats={data.snapshotStats}
        isLoading={data.isLoading?.snapshots}
      />
    </div>
  );
}

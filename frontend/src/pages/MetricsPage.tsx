import MetricsPanel from '../components/dashboard/MetricsPanel';

export default function MetricsPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6">
      <MetricsPanel metrics={data.metrics} />
    </div>
  );
}

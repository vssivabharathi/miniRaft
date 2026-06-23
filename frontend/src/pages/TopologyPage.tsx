import TopologyGraph from '../components/dashboard/TopologyGraph';

export default function TopologyPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6 h-[calc(100vh-160px)]">
      <TopologyGraph cluster={data.cluster} activeRpc={data.activeRpc} metrics={data.metrics} />
    </div>
  );
}

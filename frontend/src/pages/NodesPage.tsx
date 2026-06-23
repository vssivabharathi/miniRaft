import NodeCards from '../components/dashboard/NodeCards';

export default function NodesPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6">
      <NodeCards cluster={data.cluster} metrics={data.metrics} />
    </div>
  );
}

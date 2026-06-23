import ExecutiveOverview from '../components/dashboard/ExecutiveOverview';
import ChaosControls from '../components/controls/ChaosControls';
import StateMachineExplorer from '../components/dashboard/StateMachineExplorer';

export default function OverviewPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6">
      <ExecutiveOverview cluster={data.cluster} metrics={data.metrics} />
      <div className="grid grid-cols-12 gap-6">
        <div className="col-span-12 lg:col-span-6 xl:col-span-4">
          <ChaosControls />
        </div>
        <div className="col-span-12 lg:col-span-6 xl:col-span-8">
          <StateMachineExplorer 
            stateMachineData={data.stateMachine} 
            isLoading={data.isLoading?.stateMachine}
          />
        </div>
      </div>
    </div>
  );
}

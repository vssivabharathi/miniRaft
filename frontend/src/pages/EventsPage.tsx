import EventTimeline from '../components/dashboard/EventTimeline';

export default function EventsPage({ data }: { data: any }) {
  return (
    <div className="flex flex-col gap-6 h-[calc(100vh-160px)]">
      <EventTimeline events={data.events} />
    </div>
  );
}

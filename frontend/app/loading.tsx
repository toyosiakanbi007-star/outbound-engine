export default function Loading() {
  return (
    <div className="flex flex-col h-full">
      <header className="flex-shrink-0 h-14 flex items-center px-6 border-b border-border bg-card">
        <div className="h-4 w-32 bg-muted animate-pulse rounded" />
      </header>
      <div className="flex-1 p-6 space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-24 bg-card border border-border rounded-lg animate-pulse" />
          ))}
        </div>
        <div className="h-64 bg-card border border-border rounded-lg animate-pulse" />
      </div>
    </div>
  );
}

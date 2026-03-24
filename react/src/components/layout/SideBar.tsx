export default function Sidebar() {
  return (
    <div className="w-60 bg-[#112240] p-4">
      <h2 className="text-sm text-gray-400 mb-4">Navigation</h2>

      <button className="block w-full text-left mb-2 hover:text-cyan-400">
        Citation Network
      </button>

      <button className="block w-full text-left mb-2 hover:text-cyan-400">
        Topic Clusters
      </button>

      <button className="block w-full text-left hover:text-cyan-400">
        Authors
      </button>
    </div>
  );
}
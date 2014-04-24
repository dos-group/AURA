package de.tuberlin.aura.core.topology;


public final class GenericDirectedGraph {

	// Disallow instantiation.
	private GenericDirectedGraph() {}


	/*public static interface Visitor<T> {
		
		public abstract void visit( final T element );
	}
	
	public static interface Visitable<T> {
		
		public abstract void accept( final Visitor<T> visitor );
	}
	
	public static interface TopologyElement<T> extends Visitable<?>{
		
		public List<T> getInputs();
		
		public List<T> getOutputs();		
	} 
	
	public static interface Topology<T extends TopologyElement<?>> {
		
		public Collection<T> getSources();
		
		public Collection<T> getSinks();		
	}
		

	public static final class TopologyBreadthFirstTraverser {
		
		public static void traverse( final AuraTopology topology, final Visitor<Node> visitor ) {
			traverse( false, topology, visitor );
		}
		
		public static void traverseBackwards( final AuraTopology topology, final Visitor<Node> visitor ) {
			traverse( true, topology, visitor );
		}
		
		@SuppressWarnings("unchecked")
		private static <T extends TopologyElement<?>> void traverse( final boolean traverseBackwards, final Topology<T> topology, final Visitor<T> visitor ) {
			// sanity check.
			if( topology == null )
				throw new IllegalArgumentException( "topology == null" );
			if( visitor == null )
				throw new IllegalArgumentException( "visitor == null" );
			
			final Set<T> visitedNodes = new HashSet<T>();									
			final Queue<T> q = new LinkedList<T>();

			final Collection<T> startNodes;
			if( traverseBackwards )
				startNodes = topology.getSinks();
			else
				startNodes = topology.getSources();
			
			for( final T node : startNodes )
				q.add( node );
			
			while(! q.isEmpty() ) { 
				final T node = q.remove();
				node.accept( visitor );				
				
				final Collection<T> nextVisitedNodes;
				if( traverseBackwards )
					nextVisitedNodes = (Collection<T>) node.getInputs();
				else
					nextVisitedNodes = (Collection<T>) node.getOutputs();				
				
				for( final T nextNode : nextVisitedNodes ) {
					if( !visitedNodes.contains( nextNode ) ) {
						q.add( nextNode );
						visitedNodes.add( nextNode );
					}
				}
			}		
		}
	}*/

}

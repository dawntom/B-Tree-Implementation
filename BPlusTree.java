import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * BPlusTree Class Assumptions: 1. No duplicate keys inserted 2. Order D:
 * D<=number of keys in a node <=2*D 3. All keys are non-negative TODO: Rename
 * to BPlusTree
 */
public class BPlusTree<K extends Comparable<K>, T> {

	public Node<K, T> root;
	public static final int D = 2;

	/**
	 * TODO Search the value for a specific key
	 * 
	 * @param key
	 * @return value
	 */
	public T search(K key) {
		return search(key, root);
	}

	/**
	 * TODO Search the value for a specific key
	 * 
	 * @param key,node
	 * @return value
	 */
	public T search(K key, Node currentRoot) {
		if (!currentRoot.isLeafNode) {
			int cutPoint = 0;
			for (int i = 0; i < currentRoot.keys.size(); i++) {
				if (((K) currentRoot.keys.get(i)).compareTo(key) > 0) {
					cutPoint = i;
					break;
				}else if(i == currentRoot.keys.size()-1)
					cutPoint = i+1;
			}
			return search(key, ((Node) ((IndexNode) currentRoot).children.get(cutPoint)));
		} else {
			int idx = currentRoot.keys.indexOf(key);
			if (idx == -1)
				return null;
			else
				return (T) ((LeafNode) currentRoot).values.get(idx);
		}
	}

	/**
	 * TODO Insert a key/value pair into the BPlusTree
	 * 
	 * @param key
	 * @return value
	 */
	public void insert(K key, T value) {
		if (root == null) { // if this is the very first insertion
			root = new LeafNode(key, value);
			root.isLeafNode = true;			
		} else {
			LeafNode targetNode = findTargetNode(key,root);
			targetNode.insertSorted(key,value);	// insert into LeafNode
			// three cases:
			if(!targetNode.isOverflowed()){ // case I: the target node has available space for one more key
				// do nothing, we have already inserted key into the LeafNode								
			}else if(targetNode.isOverflowed()){ 
				List<Node> temp = new ArrayList<Node>();
				List<Node> listOfNodes = generateList(key, root, temp); // helper function generateList() gives us a list of Nodes from root to targetNode
				Entry<K, Node<K, T>> entry = splitLeafNode(targetNode, (K) targetNode.keys.get(targetNode.keys.size()/2)); // split LeafNode
				IndexNode parent;
				if(listOfNodes.size() > 0){
					parent = (IndexNode) listOfNodes.get(listOfNodes.size()-1); // if there is an existing parent 
				}else{
					parent = new IndexNode(entry.getKey(),targetNode,entry.getValue()); // if there is no parent, create a new one
				}				
				
				if(parent.keys.size() < 2*D){ // case II: the target node is full, but its parent has space for one more key. (leaf overflow)
					if(listOfNodes.size() == 0 ){ // if no parent, we create a new one, and assign root to it
						root = parent;
					}else{ // if there is at least a parent Node, we will insert into it
						int insertPoint = 0;	
						
						// find insertion place
						if(((Comparable<K>) parent.keys.get(0)).compareTo(entry.getKey()) >= 0){
							insertPoint = 0;
						}else if(((Comparable<K>) parent.keys.get(parent.keys.size()-1)).compareTo(entry.getKey()) < 0){
							insertPoint = parent.keys.size();
						}else{
							for(int i=0;i<parent.keys.size();i++){
								if(((Comparable<K>) parent.keys.get(i)).compareTo(entry.getKey()) > 0){
									insertPoint = i;
								}
							}
						}
						parent.insertSorted(entry, insertPoint); // insert newly splitted right LeafNode into parent IndexNode
					}					
				}else{ // case III: the target node and its parent are both full. (leaf overflow and index node overflow)					
					int indexOfListOfNodes = listOfNodes.size()-2;
					while(parent.keys.size() >= 2*D){
						int insertPoint = 0;	
						
						// find insertion place
						if(((Comparable<K>) parent.keys.get(0)).compareTo(entry.getKey()) >= 0){
							insertPoint = 0;
						}else if(((Comparable<K>) parent.keys.get(parent.keys.size()-1)).compareTo(entry.getKey()) < 0){
							insertPoint = parent.keys.size();
						}else{
							for(int i=0;i<parent.keys.size();i++){
								if(((Comparable<K>) parent.keys.get(i)).compareTo(entry.getKey()) > 0){
									insertPoint = i;
								}
							}
						}
						
						parent.insertSorted(entry, insertPoint); // insert newly splitted right Node into parent IndexNode
						entry = splitIndexNode(parent,(K) parent.keys.get(parent.keys.size()/2)); // split IndexNode 
						if(indexOfListOfNodes >= 0){
							parent = (IndexNode) listOfNodes.get(indexOfListOfNodes); // parent now is the parent of current IndexNode							
						}else{
							parent = new IndexNode(entry.getKey(),parent,entry.getValue()); // no parent anymore, this is the new root
							root = parent;
							break;
						}											
						indexOfListOfNodes -= 1; // get the next parent
					}
				}
			}
		}

	}

	/**
	 * TODO Find the target node for inserting/deleting a key
	 * 
	 * @param key,currentRoot           
	 * @return LeafNode
	 */
	public LeafNode findTargetNode(K key, Node currentRoot) {
		if (!currentRoot.isLeafNode) {
			int cutPoint = 0;
			for (int i = 0; i < currentRoot.keys.size(); i++) {
				if (((K) currentRoot.keys.get(i)).compareTo(key) > 0) {
					cutPoint = i;
					break;
				}else if(i == currentRoot.keys.size()-1)
					cutPoint = i+1;
			}
			return findTargetNode(key, ((Node) ((IndexNode) currentRoot).children.get(cutPoint)));
		} else
			return (LeafNode) currentRoot;					
	}

	/**
	 * TODO Return a list of nodes from root to the node in which key is
	 * 
	 * @param key, currentRoot, listOfNodes           
	 * @return List<Node>
	 */
	public List<Node> generateList(K key, Node currentRoot, List<Node> listOfNodes) {
		if (!currentRoot.isLeafNode) {
			int cutPoint = 0;
			for (int i = 0; i < root.keys.size(); i++) {
				if (((K) currentRoot.keys.get(i)).compareTo(key) >= 0) {
					cutPoint = i;
					break;
				}else if(i == root.keys.size()-1)
					cutPoint = i+1;
			}
			listOfNodes.add(currentRoot);
			return generateList(key, ((Node) ((IndexNode) currentRoot).children.get(cutPoint)), listOfNodes);
		} else
			return listOfNodes;
	}
	
	/**
	 * TODO Split a leaf node and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param leaf, key
	 * @return the key/node pair as an Entry
	 */
	public Entry<K, Node<K, T>> splitLeafNode(LeafNode<K, T> leaf, K key) {
		LeafNode<K, T> newRightNode;
		List<K> newKeys;
		List<T> newValues;
		int cutPoint = 0;
		for (int i = 0; i < leaf.keys.size(); i++) {
			if ((leaf.keys.get(i)).compareTo(key) < 0)
				continue;
			else {
				cutPoint = i;
				break;
			}
		}
		// create new right node
		newKeys = leaf.keys.subList(cutPoint, leaf.keys.size());
		newValues = leaf.values.subList(cutPoint, leaf.values.size());
		newRightNode = new LeafNode<K, T>(newKeys, newValues);

		// split by removing right side
		leaf.keys = new ArrayList<K>(leaf.keys.subList(0, cutPoint));
		leaf.values = new ArrayList<T>(leaf.values.subList(0, cutPoint));

		return new AbstractMap.SimpleEntry<K, Node<K, T>>(key, (Node) newRightNode);
	}

	/**
	 * TODO split an indexNode and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param index, any other relevant data
	 * @return new key/node pair as an Entry
	 */
	public Entry<K, Node<K, T>> splitIndexNode(IndexNode<K, T> index, K key) {
		IndexNode<K, T> newRightNode;
		List<K> newKeys;
		List<Node<K, T>> newChildren;
		int cutPoint = 0;
		for (int i = 0; i < index.keys.size(); i++) {
			if ((index.keys.get(i)).compareTo(key) < 0)
				continue;
			else {
				cutPoint = i;
				break;
			}
		}
		// create new right node
		newKeys = index.keys.subList(cutPoint+1, index.keys.size());
		newChildren = index.children.subList(cutPoint+1, index.children.size());
		newRightNode = new IndexNode<K, T>(newKeys, newChildren);

		// split by removing right side
		index.keys = new ArrayList<K>(index.keys.subList(0, cutPoint));
		index.children = new ArrayList<Node<K, T>>(index.children.subList(0, cutPoint+1));

		return new AbstractMap.SimpleEntry<K, Node<K, T>>(key, (Node) newRightNode);
	}

	/**
	 * TODO Delete a key/value pair from this B+Tree
	 * 
	 * @param key
	 */
	public void delete(K key) {
		LeafNode targetNode = findTargetNode(key,root); 
		targetNode.deleteSorted(key); // delete the key in target node
		// three cases:
		if(!targetNode.isUnderflowed()){ // case I: the target node is not underflowed (at least half full), there is nothing left to do.
			// do nothing, we have already deleted key in the LeafNode
		}else if(targetNode.isUnderflowed()){
			List<Node> temp = new ArrayList<Node>();
			List<Node> listOfNodes = generateList(key, root, temp); // helper function generateList() gives us a list of Nodes from root to targetNode
			IndexNode parent = null;
			if(listOfNodes.size() > 0){
				parent = (IndexNode) listOfNodes.get(listOfNodes.size()-1); // if there is an existing parent 
			}else{
				 // if there is no parent
			}
			
			updateLeafNode(root); // update previousLeaf and nextLeaf for each LeafNode
			int whatWeDo = 0; // determine what to do next, depending on whether return is -1 or not
			if(targetNode.previousLeaf != null){ // if target node has left sibling
				whatWeDo = handleLeafNodeUnderflow(targetNode.previousLeaf,targetNode,parent);
			}else{
				whatWeDo = handleLeafNodeUnderflow(targetNode,targetNode.nextLeaf,parent);
			}
			
			if(whatWeDo == -1){ // case II: we redistribute evenly between target node and its sibling, and update key in parent
				// redistributed in handleLeafNodeUnderflow(), all done
			}else{ // case III: merge the node with the target sibling
                // we have already merged in previously called handleLeafNodeUnderflow()
                parent.keys.remove(whatWeDo); // remove the splitkey
                parent.children.remove(whatWeDo); // remove the corresponding child of that splitkey
                
                updateLeafNode(root);
                int indexOfListOfNodes = listOfNodes.size()-2;
                while(parent.isUnderflowed()){ // if parent is underflowed after deleting splitkey
                    IndexNode leftIndex = null;
                    IndexNode rightIndex = null;
                    if(parent == root){ // this is root, do nothing
                    	break;
                    }else if(((IndexNode)listOfNodes.get(indexOfListOfNodes)).children.indexOf(parent) != 0){ // if we have a left sibling                        
                        rightIndex = parent; // update left, right, and parent
                        parent = (IndexNode) listOfNodes.get(indexOfListOfNodes);
                        leftIndex = (IndexNode) parent.children.get(parent.children.indexOf(rightIndex)-1);                       
                    }else{ // no left sibling, use right sibling
                        leftIndex = parent;
                        parent = (IndexNode) listOfNodes.get(indexOfListOfNodes);
                        rightIndex = (IndexNode) parent.children.get(parent.children.indexOf(leftIndex)+1);  
                    }
                    whatWeDo = handleIndexNodeUnderflow(leftIndex,rightIndex,parent); // redistribute or merge the underflowed IndexNode
                    
                    if(whatWeDo == -1){ // case II: we redistribute evenly between target node and its sibling, and update key in parent
                        // redistributed in handleLeafNodeUnderflow(), all done
                        break;
                    }else{ // case III: merge the node with the target sibling
                        // merged in handleLeafNodeUnderflow()
                        parent.keys.remove(whatWeDo); // remove the splitkey
                        parent.children.remove(whatWeDo); // remove the corresponding child of that splitkey
                        
                        if(root == parent && parent.keys.size() == 0){ // if merge cause root to be empty
                            root = (Node<K, T>) parent.children.get(0); // update root 
                            parent = null; // delete the empty root
                        }
                    }
                    indexOfListOfNodes -= 1; // get the next parent
                    if(indexOfListOfNodes >= 0){
                        parent = (IndexNode) listOfNodes.get(indexOfListOfNodes); // parent now is the parent of current IndexNode							
                    }else{                        
                        break;
                    }											
                    
                }
            }
		}
	}

	/**
	 * TODO Handle LeafNode Underflow (merge or redistribution)
	 * 
	 * @param left: the smaller node
	 * @param right: the bigger node
	 * @param parent: their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleLeafNodeUnderflow(LeafNode<K, T> left, LeafNode<K, T> right, IndexNode<K, T> parent) {
		if(left.keys.size() > D || right.keys.size() > D){ // sibling have extra keys to share, redistribute
			if(left.keys.size() > D){ // left has extra to share
                int cutPoint = 0; // the index of key in parent to be updated
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(right.keys.get(0)) >= 0){
                        cutPoint = i;
                        break;
                    }
				}
                
				int total = left.keys.size() + right.keys.size();
				int leftHas = total/2;
				
				right.keys.addAll(0,new ArrayList<K>(left.keys.subList(leftHas, left.keys.size()))); // right receive
				right.values.addAll(0,new ArrayList<T>(left.values.subList(leftHas, left.values.size())));
				
				left.keys = new ArrayList<K>(left.keys.subList(0, leftHas)); // left give
				left.values = new ArrayList<T>(left.values.subList(0, leftHas));		
                
                parent.keys.set(cutPoint,right.keys.get(0));
			}else if(right.keys.size() > D){ // right has extra to share	
                int cutPoint = 0; // the index of key in parent to be updated
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(right.keys.get(0)) >= 0){
                        cutPoint = i;
                        break;
                    }						
				}
                
				int total = left.keys.size() + right.keys.size();
				int leftHas = total/2;
				int rightHas = total - leftHas;
				
				left.keys.addAll(new ArrayList<K>(right.keys.subList(0, right.keys.size()-rightHas))); // left receive
				left.values.addAll(new ArrayList<T>(right.values.subList(0, right.values.size()-rightHas)));
				
				right.keys = new ArrayList<K>(right.keys.subList(right.keys.size()-rightHas, right.keys.size())); // right give
				right.values = new ArrayList<T>(right.values.subList(right.values.size()-rightHas, right.values.size()));
                
                parent.keys.set(cutPoint,right.keys.get(0));
			}	
			return -1;
		}else{ // sibling does not have enough keys to share, merge 
			if(left.isUnderflowed()){ // left is underflowed, merge to right
				int cutPoint = 0; // the index of key in parent node to be deleted
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(right.keys.get(0)) >= 0){
                        cutPoint = i;
                        break;
                    }						
				}
				
				right.keys.addAll(0,new ArrayList<K>(left.keys.subList(0, left.keys.size()))); // right receive all left nodes
				right.values.addAll(0,new ArrayList<T>(left.values.subList(0, left.values.size())));
				
				right.previousLeaf = left.previousLeaf; // horizontally connect
				if(left.previousLeaf != null){
					left.previousLeaf.nextLeaf = right;
				}				
				left = null; // delete left
				
				return cutPoint;
			}else{ // right is underflowed, merge to left
				int cutPoint = 0; // the index of key in parent node to be deleted
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(left.keys.get(left.keys.size()-1)) >= 0){
                        cutPoint = i;
                        break;
                    }						
				}
				
				left.keys.addAll(new ArrayList<K>(right.keys.subList(0, right.keys.size()))); // left receive all right nodes
				left.values.addAll(new ArrayList<T>(right.values.subList(0, right.values.size())));
				
				left.nextLeaf = right.nextLeaf; // horizontally connect
				if(right.nextLeaf != null){
					right.nextLeaf.previousLeaf = left;
				}				
				right = null; // delete right
				
				return cutPoint;
			}			
		}		
	}

	/**
	 * TODO Handle IndexNode Underflow (merge or redistribution)
	 * 
	 * @param left: the smaller node
	 * @param right: the bigger node
	 * @param parent: their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleIndexNodeUnderflow(IndexNode<K, T> leftIndex, IndexNode<K, T> rightIndex, IndexNode<K, T> parent) {
		if(leftIndex.keys.size() > D || rightIndex.keys.size() > D){ // sibling have extra keys to share, redistribute
			if(leftIndex.keys.size() > D){ // left has extra to share
				int total = leftIndex.keys.size() + rightIndex.keys.size();
				int leftHas = total/2;
				
				rightIndex.keys.addAll(0,new ArrayList<K>(leftIndex.keys.subList(leftHas, leftIndex.keys.size()))); // rightIndex receive
				rightIndex.children.addAll(0,new ArrayList<Node<K, T>>(leftIndex.children.subList(leftHas, leftIndex.children.size())));
				
				leftIndex.keys = new ArrayList<K>(leftIndex.keys.subList(0, leftHas)); // leftIndex give
				leftIndex.children = new ArrayList<Node<K, T>>(leftIndex.children.subList(0, leftHas));		
				
				int swapKeyIdx = parent.children.indexOf(leftIndex); // swap key between parent and rightIndex
				K tmp = parent.keys.get(swapKeyIdx); 			
				parent.keys.set(swapKeyIdx,rightIndex.keys.get(0)); // replace key in parent 
				rightIndex.keys.remove(0); // remove key in rightIndex  
				for(int i=0;i<rightIndex.keys.size();i++){ // add key in rightIndex at correct position
					if(rightIndex.keys.get(i).compareTo(tmp) > 0){
						rightIndex.keys.add(i,tmp);
						break;
					}
				}
			}else if(rightIndex.keys.size() > D){ // right has extra to share	
				int total = leftIndex.keys.size() + rightIndex.keys.size();
				int leftHas = total/2;
				int rightHas = total - leftHas;
				
				leftIndex.keys.addAll(new ArrayList<K>(rightIndex.keys.subList(0, rightIndex.keys.size()-rightHas))); // leftIndex receive
				leftIndex.children.addAll(new ArrayList<Node<K, T>>(rightIndex.children.subList(0, rightIndex.children.size()-rightHas)));
				
				rightIndex.keys = new ArrayList<K>(rightIndex.keys.subList(rightIndex.keys.size()-rightHas, rightIndex.keys.size())); // rightIndex give
				rightIndex.children = new ArrayList<Node<K, T>>(rightIndex.children.subList(rightIndex.children.size()-rightHas, rightIndex.children.size()));		
				
				int swapKeyIdx = parent.children.indexOf(leftIndex); // swap key between parent and rightIndex
				K tmp = parent.keys.get(swapKeyIdx); 			
				parent.keys.set(swapKeyIdx,leftIndex.keys.get(leftIndex.keys.size()-1)); // replace key in parent 
				leftIndex.keys.remove(leftIndex.keys.size()-1); // remove key in leftIndex  
				for(int i=0;i<leftIndex.keys.size();i++){ // add key in leftIndex at correct position
					if(leftIndex.keys.get(i).compareTo(tmp) > 0){
						leftIndex.keys.add(i,tmp);
						break;
					}
				}
			}	
			return -1;
		}else{
			if(leftIndex.isUnderflowed()){ // leftIndex is underflowed, merge to right
				int cutPoint = 0; // the index of key in parent to be deleted
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(rightIndex.keys.get(0)) >= 0){
						cutPoint = i;
						break;
					}						
				}
				
				rightIndex.keys.add(0,parent.keys.get(cutPoint)); // merge parent

				rightIndex.keys.addAll(0,new ArrayList<K>(leftIndex.keys.subList(0, leftIndex.keys.size()))); // merge leftIndex
				rightIndex.children.addAll(0,new ArrayList<Node<K, T>>(leftIndex.children.subList(0, leftIndex.children.size())));
				
                leftIndex = null; // delete leftIndex
                
				return cutPoint;
			}else{ // rightIndex is underflowed, merge to left
				int cutPoint = 0; // the index of key in parent to be deleted
				for(int i=0;i<parent.keys.size();i++){
					if(parent.keys.get(i).compareTo(leftIndex.keys.get(leftIndex.keys.size()-1)) >= 0){
						cutPoint = i;
						break;
					}						
				}
				
				leftIndex.keys.add(parent.keys.get(cutPoint)); // merge parent

				leftIndex.keys.addAll(new ArrayList<K>(rightIndex.keys.subList(0, rightIndex.keys.size()))); // merge rightIndex
				leftIndex.children.addAll(new ArrayList<Node<K, T>>(rightIndex.children.subList(0, rightIndex.children.size())));
				
                rightIndex = null; // delete rightIndex
                
				return cutPoint;
			}			
		}
	}
	
	/**
	 * TODO Update the nextLeaf and previousLeaf for each LeafNode
	 * @param root
	 */
	public void updateLeafNode(Node root){
		LinkedBlockingQueue<Node<K, T>> queue;

		// Create a queue to hold node pointers.
		queue = new LinkedBlockingQueue<Node<K, T>>();
		
		ArrayList<LeafNode> leaves = new ArrayList<LeafNode>();
		if (root == null) {
			return;
		}
		queue.add(root);
		while (!queue.isEmpty()) { // do BFS, put all LeafNodes into an ArrayList
			Node<K, T> target = queue.poll();
			if (target.isLeafNode) {
				LeafNode<K, T> leaf = (LeafNode<K, T>) target;
				leaves.add(leaf);				
			} else {
				IndexNode<K, T> index = ((IndexNode<K, T>) target);				
				queue.addAll(index.children);
			}			
		}
		
		if(leaves.size() == 0 || leaves.size() == 1){
			return;
		}else{
			for(int i=0;i<leaves.size();i++){
				if(i == 0){
					leaves.get(i).nextLeaf = leaves.get(i+1);
				}else if(i == leaves.size()-1){
					leaves.get(i).previousLeaf = leaves.get(i-1);
				}else{
					leaves.get(i).nextLeaf = leaves.get(i+1);
					leaves.get(i).previousLeaf = leaves.get(i-1);
				}
			}
		}		
	}
}

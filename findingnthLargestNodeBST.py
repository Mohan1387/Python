class Node:
    # A utility function to create a new node
    def __init__(self ,key):
        self.val = key
        self.left = None
        self.right = None
 
nth = 3
# Create an empty queue for level order traversal
queue = []
         
count = 0

def print_tree_in(rt): 
  if rt != None:
    _print_tree_in(rt)
  else:
    return 0 
        
def _print_tree_in(cur_node):
  if cur_node != None:
    _print_tree_in(cur_node.left)
    if len(queue) < nth:
        queue.append(cur_node.val)
    else:
        return
    _print_tree_in(cur_node.right) 

   
# Driver Program to test above function
root = Node(10)
root.left = Node(5)
root.right = Node(12)
root.left.left = Node(2)
root.right.left = Node(11)
root.right.right = Node(14)
 
 
print_tree_in(root)

#for BST
print(queue[nth-1])
#if not a BST 
print(sorted(queue)[nth-1])
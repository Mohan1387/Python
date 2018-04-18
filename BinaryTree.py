import time
class node():
  def __init__(self,val=None):
    self.val=val
    self.left=None
    self.right=None

class BinaryTree():
  def __init__(self):
    self.root=None
  
  def insert(self, cur_val):
    if self.root==None:
      self.root = node(cur_val)
    else:
      self._insert(cur_val,self.root)
      
  def _insert(self,cur_val,cur_node):
    if  cur_val < cur_node.val:
        if cur_node.left == None:
          cur_node.left = node(cur_val)
        else:
          self._insert(cur_val,cur_node.left)
    elif cur_val > cur_node.val:
        if cur_node.right == None:
          cur_node.right = node(cur_val)
        else:
          self._insert(cur_val,cur_node.right)
    else:
      print("Value is in Tree Already")
  
  def print_tree_in(self):
    if self.root != None:
      self._print_tree_in(self.root)
      
  def _print_tree_in(self,cur_node):
    if cur_node != None:
      self._print_tree_in(cur_node.left)
      #time.sleep(5)
      print(str(cur_node.val))
      self._print_tree_in(cur_node.right)
    
  def print_tree_pre(self):
    if self.root != None:
      self._print_tree_pre(self.root)
      
  def _print_tree_pre(self,cur_node):
    if cur_node != None:
      print(str(cur_node.val))
      self._print_tree_pre(cur_node.left)
      self._print_tree_pre(cur_node.right)
      
  def print_tree_post(self):
    if self.root != None:
      self._print_tree_post(self.root)
      
  def _print_tree_post(self,cur_node):
    if cur_node != None:
      self._print_tree_post(cur_node.left)
      self._print_tree_post(cur_node.right)
      print(str(cur_node.val))
#    10
#  5    12
#2    11
tree = BinaryTree()
tree.insert(10)
tree.insert(5)
tree.insert(2)
tree.insert(12)
tree.insert(11)
print("Inorder Traversal")
#[2,5,10,11,12]
tree.print_tree_in()
print("Preorder Traversal")
#[10,5,2,11,12]
tree.print_tree_pre()
print("Postorder Traversal")
#[2,11,5,12,10]
tree.print_tree_post()
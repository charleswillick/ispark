package happy.istudy.algorithm.bst

/**
  * Created by zqwu on 2017/3/15 0015.
  */
class BinarySearchTree[T <% Comparable[T]] {
  var _root:Node[T] = null
  //var _high = 0
  //var _length = 0

  //def high = _high
  //def lenth = _length
  def root = _root

  def this(node:Node[T]){
    this()
    this._root = node
  }

  def insert(key:T): Unit ={
    val node = new Node[T](key)
    this.insert(node)
  }

  def insert(node:Node[T]): Unit ={
    var y:Node[T] = null
    var x = root
    //y is the father of x
    while(x != null){
      y = x
      if(node.key.compareTo(x.key) < 0){
        x = x.left
      }
      else {
        x = x.right
      }
    }

    node.setFather(y)

    if(y == null){
      this._root = node
    }else if(node.key.compareTo(y.key) < 0){
      y.setLeft(node)
    }else{
      y.setRight(node)
    }

    //this._length += 1

  }

  def printTree(): Unit ={
    val root = this.root
    if(root.left!= null) new BinarySearchTree[T](root.left).printTree()
    println(root.key)
    if(root.right!= null) new BinarySearchTree[T](root.right).printTree()
}

}

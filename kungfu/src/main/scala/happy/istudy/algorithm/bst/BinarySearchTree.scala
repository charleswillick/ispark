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

  /**
    * 按中序遍历的方法打印所有节点的值
    */
  def printTree(): Unit ={
    val root = this.root
    printNode(root)
  }

  /**
    * 按中序遍历打印节点的值和左右节点
    * @param node
    */
  private def printNode(node: Node[T]): Unit ={
    inorderTravel(node,(x:Node[T]) => println(x.key))
  }

  /**
    * 中序遍历一棵树
    * @param func 遍历节点的方法
    */
  def inorderTravel(func:Node[T] => Any): Unit ={
    val root = this.root
    inorderTravel(root,func)
  }

  /**
    * 前序遍历
    * @param func
    */
  def preorderTravel(func:Node[T] => Any): Unit ={
    val root = this.root
    preorderTravel(root,func)
  }

  /**
    * 后续遍历一棵树
    * @param func
    */
  def postorderTravel(func:Node[T] => Any): Unit ={
    val root = this.root
    postorderTravel(root,func)
  }

  private def inorderTravel(node: Node[T],func:Node[T] => Any): Unit ={
    if(node != null){
      if(node.left != null) inorderTravel(node.left,func)
      func(node)
      if(node.right != null) inorderTravel(node.right,func)
    }
  }

  private def preorderTravel(node: Node[T],func:Node[T] => Any): Unit ={
    if(node != null){
      func(node)
      if(node.left != null) preorderTravel(node.left,func)
      if(node.right != null) preorderTravel(node.right,func)
    }
  }

  private def postorderTravel(node: Node[T],func:Node[T] => Any): Unit ={
    if(node != null){
      if(node.left != null) postorderTravel(node.left,func)
      if(node.right != null) postorderTravel(node.right,func)
      func(node)
    }
  }

}

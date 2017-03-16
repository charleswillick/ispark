package happy.istudy.algorithm.bst

/**
  * Created by zqwu on 2017/3/15 0015.
  */
class Node [T]{
  private var _key:T = _
  private var _left:Node[T] = _
  private var _right:Node[T] = _
  private var _father:Node[T] = _
  def key = _key
  def left = _left
  def right = _right
  def father = _father

  def this(keyValue:T){
    this()
    this._key = keyValue
    this._left = null
    this._right = null
    this._father = null
  }

  def apply(key:T): Node[T] ={
    this(key)
  }

  def setFather(node:Node[T]): Unit ={
    this._father = node
  }

  def setLeft(node:Node[T]): Unit ={
    this._left = node
  }

  def setRight(node:Node[T]): Unit ={
    this._right = node
  }

}

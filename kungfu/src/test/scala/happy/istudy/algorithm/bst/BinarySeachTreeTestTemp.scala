package happy.istudy.algorithm.bst

/**
  * Created by zqwu on 2017/3/16 0016.
  */
object BinarySeachTreeTestTemp {
  def main(args: Array[String]) {
    val tree = new BinarySearchTree[Int]()
    tree.insert(3)
    tree.insert(3)
    tree.insert(4)
    tree.insert(5)
    tree.insert(1)
    tree.insert(2)
    tree.insert(6)

    tree.printTree()
  }

}

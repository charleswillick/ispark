package happy.istudy.algorithm.bst

/**
  * Created by zqwu on 2017/3/16 0016.
  */
object BinarySeachTreeTestTemp {
  def main(args: Array[String]) {
    val tree = new BinarySearchTree[Int]()
    tree.insert(3)
    tree.insert(4)
    tree.insert(5)
    tree.insert(1)
    tree.insert(2)
    tree.insert(6)

    println("中序打印：")
    tree.inorderTravel((x:Node[Int]) => {print(x.key);print(" ")})
    println("\n前序打印：")
    tree.preorderTravel((x:Node[Int]) => {print(x.key);print(" ")})
    println("\n后序打印：")
    tree.postorderTravel((x:Node[Int]) => {print(x.key);print(" ")})
  }

}

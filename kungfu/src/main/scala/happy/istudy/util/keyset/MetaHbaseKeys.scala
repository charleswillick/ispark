package happy.istudy.util.keyset

/**
  * Created by zqwu on 2016/3/29 0029.
  */
object MetaHbaseKeys {
  val LDACATEGORYSET = "cf:lda_category_set"
  val KEYWORDCATEGORYSET = "cf:keyword_category_set"
  val NOLDACATEGORYINDEX = "cf:no_lda_category_index"
  val INDEXCOLUMN = "cf:map"
  val MAX = "cf:max"
  val SEP = "~"
  val CSEP = ":"
  val VERSION_CONTROL_ROWKEY = "version"
  val VERSION_CONTROL_ONLINE = "cf:online"
  val VERSION_CONTROL_INIT = "cf:init"
  val META_CHANNEL_ROWKEY = "channel"
  val META_CATEGORY_ROWKEY = "category"
  val META_SOURCE_ROWKEY = "source"
}

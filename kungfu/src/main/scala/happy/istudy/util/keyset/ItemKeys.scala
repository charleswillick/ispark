package happy.istudy.util.keyset

/**
  * Created by zqwu on 2016/3/23 0023.
  */
object ItemKeys {
  val LIBSVM_INTER_SEP = " "
  val LIBSVM_INNER_SEP = ":"
  val SEGMENT = "segment"
  val CHANNEL = "channel"
  val CHANNELID = "channelid"
  val PCHANNEL = "pchannel"
  val TITLE = "title"
  val CATE = "category"
  val PCATE = "pcategory"
  val SOURCE = "source"
  val TITLE_SEGMENT = "title_segment"
  val CHANNEL_SOURCE = "channel_source"
  val DOCID = "docid"
  val CONTENT = "content"
  val ALLCONTENT = "allcontent"
  val DEFAULT_CHANNEL = "未分类"
  val DEFAULT_CHANNELID = "0000000"
  val CF_PREFIX = "cf:"
  val MEAT_CF_PREFIX = "cf:"
  val RSOURCENAME = "rsourcename"
  val CTM = "ctm"
  val UP = "up"
  val DOWN = "down"
  val LIKE = "like"
  val CTYPE = "ctype"
  val COMMENT = "comment"
  val PUBLISHTIME = "publishtime"
  val KEYWORD = "keyword"
  val MODELJSON = "modeljson"
  val PERSON = "person"
  val AREA= "area"
  val ISHOT= "ishot"
  val SAVECONFIG = "saveconfig"
  val LENGTH = "length"
  val ALLLENGTH = "alllength"



  val PROFILE_PREFIX = "cf:p_"
  val PROFILE = "cf:profile"
  val PROFILE_TOPIC = "cf:p_topic" //主题
  val PROFILE_TOPIC_CTR = "cf:p_tpc" //主题用于ctr
  val PROFILE_KEYWORD = "cf:p_kw" //关键词
  val PROFILE_KEYWORD_SHORT = "cf:p_kw_short" //关键词
  val PROFILE_CHANNEL = "cf:p_chl" //频道
  val PROFILE_CATEGORY = "cf:p_ctgr" //顶级分类
  val PROFILE_SOURCE = "cf:p_src" //新闻来源
  val PROFILE_SOURCE_SHORT = "cf:p_src_short" //新闻来源
  val PROFILE_SOURCER = "cf:p_srcr" //新闻来源,用于推荐
  val PROFILE_SOURCER_SHORT = "cf:p_srcr_short" //新闻来源,用于推荐
  val PROFILE_CTR = "cf:p_ctr" //给ctr的LR用
  val PROFILE_RECOMMANDATION = "cf:p_rec" //给基于用户关键词的推荐算法使用
  val PROFILE_RECOMMANDATION_SHORT = "cf:p_rec_short" //给基于用户关键词的推荐算法使用
}

class MovieEntry(id:String, title:String, count: Int) {
  var mID: String = id;
  var mTitle: String = title;
  var rateCount: Int = count;

  def +(that: MovieEntry) : MovieEntry = {
    if (mID != that.mID) return this
    mTitle += that.mTitle;
    rateCount += that.rateCount;
    return this
  }


  override def toString(): String = {
    return count.toString + "   " + mTitle;
  }
}

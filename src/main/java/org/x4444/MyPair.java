package org.x4444;

public class MyPair<T1, T2> {

  public T1 t1;
  public T2 t2;

  public MyPair(T1 t1, T2 t2) {
    this.t1 = t1;
    this.t2 = t2;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((t1 == null) ? 0 : t1.hashCode());
    result = prime * result + ((t2 == null) ? 0 : t2.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("rawtypes")
    MyPair other = (MyPair) obj;
    if (t1 == null) {
      if (other.t1 != null)
        return false;
    } else if (!t1.equals(other.t1))
      return false;
    if (t2 == null) {
      if (other.t2 != null)
        return false;
    } else if (!t2.equals(other.t2))
      return false;
    return true;
  }

}

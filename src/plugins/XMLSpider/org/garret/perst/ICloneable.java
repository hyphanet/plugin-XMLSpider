package plugins.XMLSpider.org.garret.perst;

/**
 * This interface allows to clone its implementor.
 * It is needed because Object.clone is protected and java.lang.Cloneable interface contains no method defintion
 */
public interface ICloneable extends Cloneable { 
    Object clone() throws CloneNotSupportedException;
}

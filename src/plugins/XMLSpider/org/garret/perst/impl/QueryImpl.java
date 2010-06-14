package plugins.XMLSpider.org.garret.perst.impl;

import java.lang.reflect.*;
import java.util.*;
import java.text.*;
import java.util.Arrays.*;

import plugins.XMLSpider.org.garret.perst.*;

class FilterIterator<T> extends IterableIterator<T> { 
    Iterator     iterator;
    Node         condition;
    QueryImpl<T> query;
    int[]        indexVar;
    long[]       intAggragateFuncValue;
    double[]     realAggragateFuncValue;
    Object[]     containsArray;
    T            currObj;
    Object       containsElem;
    
    final static int maxIndexVars = 32;
    
    public boolean hasNext() { 
        if (currObj != null) { 
            return true;
        }
        while (iterator.hasNext()) { 
            Object obj = iterator.next();
            if (query.cls.isInstance(obj)) { 
                currObj = (T)obj;
                if (condition == null) { 
                    return true;
                }
                try { 
                    if (condition.evaluateBool(this)) {
                        return true;
                    }
                } catch (JSQLRuntimeException x) { 
                    query.reportRuntimeError(x);
                }
                currObj = null;
            }
        }
        return false;
    }
    
    public T next() { 
        if (!hasNext()) { 
            throw new NoSuchElementException();
        }
        T obj = currObj;
        currObj = null;
        return obj;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
        
    
    FilterIterator(QueryImpl query, Iterator<T> iterator, Node condition) { 
        this.query = query;
        this.iterator = iterator;
        this.condition = condition;
        indexVar = new int[maxIndexVars];
    }
}



class Node { 
    int type;
    int tag;

    final static int tpBool       = 0;
    final static int tpInt        = 1;
    final static int tpReal       = 2;
    final static int tpFreeVar    = 3;
    final static int tpList       = 4;
    final static int tpObj        = 5;
    final static int tpStr        = 6;
    final static int tpDate       = 7;
    final static int tpArrayBool  = 8;
    final static int tpArrayChar  = 9;
    final static int tpArrayInt1  = 10;
    final static int tpArrayInt2  = 11;
    final static int tpArrayInt4  = 12;
    final static int tpArrayInt8  = 13;
    final static int tpArrayReal4 = 14;
    final static int tpArrayReal8 = 15;
    final static int tpArrayStr   = 16;
    final static int tpArrayObj   = 17;
    final static int tpCollection = 18;
    final static int tpUnknown    = 19;
    final static int tpAny        = 20;

    final static String typeNames[] = {
        "boolean", 
        "integer", 
        "real", 
        "index variable", 
        "list", 
        "object",
        "string",
        "date",
        "array of boolean",
        "array of char",
        "array of byte",
        "array of short",
        "array of int",
        "array of long",
        "array of float",
        "array of double",
        "array of string",
        "array of object",
        "collection",
        "unknown",
        "any"
    };

    final static int opNop     = 0;
    final static int opIntAdd  = 1;
    final static int opIntSub  = 2;
    final static int opIntMul  = 3;
    final static int opIntDiv  = 4;
    final static int opIntAnd  = 5;
    final static int opIntOr   = 6;
    final static int opIntNeg  = 7;
    final static int opIntNot  = 8;
    final static int opIntAbs  = 9;
    final static int opIntPow  = 10;
    final static int opIntEq   = 11;
    final static int opIntNe   = 12;
    final static int opIntGt   = 13;
    final static int opIntGe   = 14;
    final static int opIntLt   = 15;
    final static int opIntLe   = 16;
    final static int opIntBetween = 17;

    final static int opRealEq   = 18;
    final static int opRealNe   = 19;
    final static int opRealGt   = 20;
    final static int opRealGe   = 21;
    final static int opRealLt   = 22;
    final static int opRealLe   = 23;
    final static int opRealBetween = 24;

    final static int opStrEq   = 25;
    final static int opStrNe   = 26;
    final static int opStrGt   = 27;
    final static int opStrGe   = 28;
    final static int opStrLt   = 29;
    final static int opStrLe   = 30;
    final static int opStrBetween = 31;
    final static int opStrLike    = 32;
    final static int opStrLikeEsc = 33;

    final static int opBoolEq   = 34;
    final static int opBoolNe   = 35;
    
    final static int opObjEq   = 36;
    final static int opObjNe   = 37;
    
    final static int opRealAdd = 38;
    final static int opRealSub = 39;
    final static int opRealMul = 40;
    final static int opRealDiv = 41;
    final static int opRealNeg = 42;
    final static int opRealAbs  = 43;
    final static int opRealPow  = 44;

    final static int opIntToReal = 45;
    final static int opRealToInt = 46;
    final static int opIntToStr = 47;
    final static int opRealToStr = 48;

    final static int opIsNull = 49;

    final static int opStrGetAt  = 50;
    final static int opGetAtBool = 51;
    final static int opGetAtChar = 52;
    final static int opGetAtInt1 = 53;
    final static int opGetAtInt2 = 54;
    final static int opGetAtInt4 = 55;
    final static int opGetAtInt8 = 56;
    final static int opGetAtReal4 = 57;
    final static int opGetAtReal8 = 58;
    final static int opGetAtStr = 59;
    final static int opGetAtObj = 60;

    final static int opLength = 61;
    final static int opExists = 62;
    final static int opIndexVar = 63;

    final static int opFalse   = 64;
    final static int opTrue    = 65;
    final static int opNull    = 66;
    final static int opCurrent = 67;

    final static int opIntConst = 68;
    final static int opRealConst = 69;
    final static int opStrConst = 70;

    final static int opInvoke = 71;

    final static int opScanArrayBool = 72;
    final static int opScanArrayChar = 73;
    final static int opScanArrayInt1 = 74;
    final static int opScanArrayInt2 = 75;
    final static int opScanArrayInt4 = 76;
    final static int opScanArrayInt8 = 77;
    final static int opScanArrayReal4 = 78;
    final static int opScanArrayReal8 = 79;
    final static int opScanArrayStr = 80;
    final static int opScanArrayObj = 81;
    final static int opInString = 82;

    final static int opRealSin = 83;
    final static int opRealCos = 84;
    final static int opRealTan = 85;
    final static int opRealAsin = 86;
    final static int opRealAcos = 87;
    final static int opRealAtan = 88;
    final static int opRealSqrt = 89;
    final static int opRealExp = 90;
    final static int opRealLog = 91;
    final static int opRealCeil = 92;
    final static int opRealFloor = 93;

    final static int opBoolAnd = 94;
    final static int opBoolOr  = 95;
    final static int opBoolNot = 96;

    final static int opStrLower = 97;
    final static int opStrUpper = 98;
    final static int opStrConcat = 99;
    final static int opStrLength = 100;

    final static int opLoad = 100;

    final static int opLoadAny = 101;
    final static int opInvokeAny = 102;

    final static int opContains = 111;
    final static int opElement   = 112;

    final static int opAvg    = 114;
    final static int opCount  = 115;
    final static int opMax    = 116;
    final static int opMin    = 117;
    final static int opSum    = 118;

    final static int opParameter = 119;
    
    final static int opAnyAdd  = 121;
    final static int opAnySub  = 122;
    final static int opAnyMul  = 123;
    final static int opAnyDiv  = 124;
    final static int opAnyAnd  = 125;
    final static int opAnyOr   = 126;
    final static int opAnyNeg  = 127;
    final static int opAnyNot  = 128;
    final static int opAnyAbs  = 129;
    final static int opAnyPow  = 130;
    final static int opAnyEq   = 131;
    final static int opAnyNe   = 132;
    final static int opAnyGt   = 133;
    final static int opAnyGe   = 134;
    final static int opAnyLt   = 135;
    final static int opAnyLe   = 136;
    final static int opAnyBetween = 137;
    final static int opAnyLength = 138;
    final static int opInAny   = 139;
    final static int opAnyToStr = 140;
    final static int opConvertAny = 141;
    
    final static int opResolve = 142;
    final static int opScanCollection = 143;

    final static int opDateEq   = 145;
    final static int opDateNe   = 146;
    final static int opDateGt   = 147;
    final static int opDateGe   = 148;
    final static int opDateLt   = 149;
    final static int opDateLe   = 150;
    final static int opDateBetween = 151;
    final static int opDateToStr   = 152;
    final static int opStrToDate   = 153;
    final static int opDateConst   = 154;

    static String wrapNullString(Object val) {
        return val == null ? "" : (String)val;
    }

    public boolean equals(Object o) { 
        return o instanceof Node && ((Node)o).tag == tag && ((Node)o).type == type;
    }
    
    static final boolean equalObjects(Object a, Object b) { 
        return a == b || (a != null && a.equals(b));
    }


    static final int getFieldType(Class type) { 
        if (type.equals(byte.class) || type.equals(short.class) || type.equals(int.class) || type.equals(long.class)) { 
            return tpInt;
        } else if (type.equals(boolean.class)) { 
            return tpBool;
        } else if (type.equals(double.class) || type.equals(float.class)) { 
            return tpReal;
        } else if (type.equals(String.class)) { 
            return tpStr;
        } else if (type.equals(Date.class)) { 
            return tpDate;
        } else if (type.equals(boolean[].class)) { 
            return tpArrayBool;
        } else if (type.equals(byte[].class)) { 
            return tpArrayInt1;
        } else if (type.equals(short[].class)) { 
            return tpArrayInt2;
        } else if (type.equals(char[].class)) { 
            return tpArrayChar;
        } else if (type.equals(int[].class)) { 
            return tpArrayInt4;
        } else if (type.equals(long[].class)) { 
            return tpArrayInt8;
        } else if (type.equals(float[].class)) { 
            return tpArrayReal4;
        } else if (type.equals(double[].class)) { 
            return tpArrayReal8;
        } else if (type.equals(String[].class)) { 
            return tpArrayStr;
        } else if (Collection.class.isAssignableFrom(type)){ 
            return tpCollection;
        } else if (type.isArray()) {
            return tpArrayObj;
        } else if (type.equals(Object.class)) {
            return tpAny;
        } else { 
            return tpObj;
        }
    }
    
    String getFieldName() { 
        return null;
    }

    boolean evaluateBool(FilterIterator t) { 
        throw new AbstractMethodError();
    }
    long    evaluateInt(FilterIterator t) {
        throw new AbstractMethodError();
    }
    double  evaluateReal(FilterIterator t) {
        throw new AbstractMethodError();
    }
    String  evaluateStr(FilterIterator t) {
        return wrapNullString(evaluateObj(t));
    }
    Date evaluateDate(FilterIterator t) {
         return (Date)evaluateObj(t);
    }
    Object  evaluateObj(FilterIterator t) {
        switch (type) { 
          case tpStr:            
            return evaluateStr(t);
          case tpDate:            
            return evaluateDate(t);
          case tpInt:
            return new Long(evaluateInt(t));
          case tpReal:
            return new Double(evaluateReal(t));
          case tpBool:
            return evaluateBool(t) ? Boolean.TRUE : Boolean.FALSE;
          default:
            throw new AbstractMethodError();
        }
    }
    Class   getType() { 
        return null;
    }
    
    public String toString() { 
        return "Node tag=" + tag + ", type=" + type;
    }

    Node(int type, int tag) {
        this.type = type;
        this.tag = tag;
    }
}

class EmptyNode extends Node { 
    boolean evaluateBool(FilterIterator t) { 
        return true;
    }

    EmptyNode() { 
        super(tpBool, opTrue);
    }
}

abstract class LiteralNode extends Node {  
    abstract Object getValue();

    Object evaluateObj(FilterIterator t) {
        return getValue();
    }

    LiteralNode(int type, int tag) { 
        super(type, tag);
    }
}
   
class IntLiteralNode extends LiteralNode { 
    long value;
    
    public boolean equals(Object o) { 
        return o instanceof IntLiteralNode && ((IntLiteralNode)o).value == value;
    }

    Object getValue() { 
        return new Long(value);
    }

    long evaluateInt(FilterIterator t) {
        return value;
    }
    
    IntLiteralNode(long value) { 
        super(tpInt, opIntConst);
        this.value = value;
    }
}


class RealLiteralNode extends LiteralNode { 
    double value;
    
    public boolean equals(Object o) { 
        return o instanceof RealLiteralNode && ((RealLiteralNode)o).value == value;
    }

    Object getValue() { 
        return new Double(value);
    }

    double evaluateReal(FilterIterator t) {
        return value;
    }
    
    RealLiteralNode(double value) { 
        super(tpReal, opRealConst);
        this.value = value;
    }
}

class StrLiteralNode extends LiteralNode { 
    String value;
    
    public boolean equals(Object o) { 
        return o instanceof StrLiteralNode && ((StrLiteralNode)o).value.equals(value);
    }

    Object getValue() { 
        return value;
    }

    String evaluateStr(FilterIterator t) {
        return value;
    }
    
    StrLiteralNode(String value) { 
        super(tpStr, opStrConst);
        this.value = value;
    }
}


class CurrentNode extends Node { 
    Class getType() {
        return cls;
    }

    Object evaluateObj(FilterIterator t) {
        return t.currObj;
    }

    CurrentNode(Class cls) { 
        super(tpObj, opCurrent);
        this.cls = cls;
    }
    Class cls;
}

class DateLiteralNode extends LiteralNode { 
    Date value;
    
    public boolean equals(Object o) { 
        return o instanceof DateLiteralNode && ((DateLiteralNode)o).value.equals(value);
    }

    Object getValue() { 
        return value;
    }

    Date evaluateDate(FilterIterator t) {
        return value;
    }
    
    DateLiteralNode(Date value) { 
        super(tpDate, opDateConst);
        this.value = value;
    }
}

class ConstantNode extends LiteralNode { 
    Object getValue() { 
        switch (tag) { 
          case opNull:
            return null;
          case opFalse:
            return Boolean.FALSE;
          case opTrue:
            return Boolean.TRUE;
          default:
            throw new Error("Invalid tag " + tag);            
        }
    }
    
    boolean evaluateBool(FilterIterator t) { 
        return tag != opFalse;
    }

    ConstantNode(int type, int tag) { 
        super(type, tag);
    }
}

class IndexOutOfRangeError extends Error {
    int loopId;

    IndexOutOfRangeError(int loop) {
        loopId = loop;
    }
}

class ExistsNode extends Node {
    Node expr;
    int  loopId;
    
    public boolean equals(Object o) { 
        return o instanceof ExistsNode && ((ExistsNode)o).expr.equals(expr) && ((ExistsNode)o).loopId == loopId;
    }

    boolean evaluateBool(FilterIterator t) { 
        t.indexVar[loopId] = 0;
        try { 
            while (!expr.evaluateBool(t)) { 
                t.indexVar[loopId] += 1;
            }
            return true;
        } catch (IndexOutOfRangeError x) { 
            if (x.loopId != loopId) { 
                throw x;
            }
            return false;
        }
    }
    
    ExistsNode(Node expr, int loopId) { 
        super(tpBool, opExists);
        this.expr = expr;
        this.loopId = loopId;
    }
}


class IndexNode extends Node {
    int loopId;
    
    public boolean equals(Object o) { 
        return o instanceof IndexNode && ((IndexNode)o).loopId == loopId;
    }

    long evaluateInt(FilterIterator t) { 
        return t.indexVar[loopId];
    }
    
    IndexNode(int loop) { 
        super(tpInt, opIndexVar);
        loopId = loop;
    }
}

class GetAtNode extends Node { 
    Node left;
    Node right;
    
    public boolean equals(Object o) { 
        return o instanceof GetAtNode && ((GetAtNode)o).left.equals(left) && ((GetAtNode)o).right.equals(right);
    }

    long evaluateInt(FilterIterator t) { 
        Object arr = left.evaluateObj(t);
        long idx = right.evaluateInt(t);

        if (right.tag == opIndexVar) { 
            try { 
                if (idx >= Array.getLength(arr)) {
                    throw new IndexOutOfRangeError(((IndexNode)right).loopId);
                }
            } catch (IllegalArgumentException x) {
                throw new Error("Argument is not array");
            }
        }
        int index = (int)idx;
        switch (tag) { 
          case opGetAtInt1:
            return ((byte[])arr)[index];
          case opGetAtInt2:
            return ((short[])arr)[index];
          case opGetAtInt4:
            return ((int[])arr)[index];
          case opGetAtInt8:
            return ((long[])arr)[index];
          case opGetAtChar:
            return ((char[])arr)[index];
          case opStrGetAt:
            return ((String)arr).charAt(index);
          default:
            throw new Error("Invalid tag " + tag);
        }
    }       

    double evaluateReal(FilterIterator t) { 
        Object arr = left.evaluateObj(t);
        long index = right.evaluateInt(t);

        if (right.tag == opIndexVar) { 
            try { 
                if (index >= Array.getLength(arr)) {
                    throw new IndexOutOfRangeError(((IndexNode)right).loopId);
                }
            } catch (IllegalArgumentException x) {
                throw new Error("Argument is not array");
            }
        }
        switch (tag) { 
          case opGetAtReal4:
            return ((float[])arr)[(int)index];
          case opGetAtReal8:
            return ((double[])arr)[(int)index];
          default:
            throw new Error("Invalid tag " + tag);
        }
    }       

    boolean evaluateBool(FilterIterator t) { 
        boolean[] arr = (boolean[])left.evaluateObj(t);
        long index = right.evaluateInt(t);

        if (right.tag == opIndexVar) { 
            try { 
                if (index >= arr.length) {
                    throw new IndexOutOfRangeError(((IndexNode)right).loopId);
                }
            } catch (IllegalArgumentException x) {              
                throw new Error("Argument is not array");
            }
        }
        return arr[(int)index];
    }

    String evaluateStr(FilterIterator t) { 
        String[] arr = (String[])left.evaluateObj(t);
        long index = right.evaluateInt(t);

        if (right.tag == opIndexVar) { 
            try { 
                if (index >= arr.length) {
                    throw new IndexOutOfRangeError(((IndexNode)right).loopId);
                }
            } catch (IllegalArgumentException x) {
                throw new Error("Argument is not array");
            }
        }
        return wrapNullString(arr[(int)index]);
    }

    Object evaluateObj(FilterIterator t) { 
        Object arr = left.evaluateObj(t);
        long index = right.evaluateInt(t);

        try { 
            if (right.tag == Node.opIndexVar) { 
                if (index >= Array.getLength(arr)) {
                    throw new IndexOutOfRangeError(((IndexNode)right).loopId);
                }
            }
            return Array.get(arr, (int)index);            
        } catch (IllegalArgumentException x) {
            throw new Error("Argument is not array");
        }
    }

    GetAtNode(int type, int tag, Node base, Node index) { 
        super(type, tag);
        left = base;
        right = index;
    }
}

class InvokeNode extends Node {
    Node target;
    Node[] arguments;
    Method mth;

    public boolean equals(Object o) { 
        return o instanceof InvokeNode 
            && equalObjects(((InvokeNode)o).target, target)
            && Arrays.equals(((InvokeNode)o).arguments, arguments)
            && equalObjects(((InvokeNode)o).mth, mth);
    }

    Class getType() { 
        return mth.getReturnType();
    }

    String getFieldName() { 
        if (target != null && target.tag != opCurrent) { 
            String baseName = target.getFieldName();
            return (baseName != null) ? baseName + "." + mth.getName() : null;
        } else { 
            return mth.getName();
        }
    }

    Object getTarget(FilterIterator t) { 
        if (target == null) { 
            return t.currObj;
        } 
        Object obj = target.evaluateObj(t);
        if (obj == null) { 
            throw new JSQLNullPointerException(target.getType(), mth.toString());
        }
        return obj;
    }

    Object[] evaluateArguments(FilterIterator t) {
        Object[] parameters = null;
        int n = arguments.length;
        if (n > 0) { 
            parameters = new Object[n];
            for (int i = 0; i < n; i++) { 
                Node arg = arguments[i];
                Object value;
                switch (arg.type) { 
                  case Node.tpInt:
                    value = new Long(arg.evaluateInt(t));
                    break;
                  case Node.tpReal:
                    value = new Double(arg.evaluateReal(t));
                    break;
                  case Node.tpStr:
                    value = arg.evaluateStr(t);
                    break;
                  case Node.tpDate:
                    value = arg.evaluateDate(t);
                    break;
                  case Node.tpBool:
                    value = new Boolean(arg.evaluateBool(t));
                    break;
                  default:
                    value = arg.evaluateObj(t);
                }
                parameters[i] = value;
            }
        }
        return parameters;
    }

    long evaluateInt(FilterIterator t) {
        Object obj = getTarget(t);
        Object[] parameters = evaluateArguments(t);
        try { 
            return ((Number)mth.invoke(obj, parameters)).longValue();
        } catch (Exception x) { 
            x.printStackTrace();
            throw new Error("Method invocation error");
        }
    }

    double evaluateReal(FilterIterator t) {
        Object obj = getTarget(t);
        Object[] parameters = evaluateArguments(t);
        try { 
            return ((Number)mth.invoke(obj, parameters)).doubleValue();
        } catch (Exception x) { 
            x.printStackTrace();
            throw new Error("Method invocation error");
        }
    }

    boolean evaluateBool(FilterIterator t) {
        Object obj = getTarget(t);
        Object[] parameters = evaluateArguments(t);
        try { 
            return ((Boolean)mth.invoke(obj, parameters)).booleanValue();
        } catch (Exception x) { 
            x.printStackTrace();
            throw new Error("Method invocation error");
        }
    }

    String evaluateStr(FilterIterator t) {
        Object obj = getTarget(t);
        Object[] parameters = evaluateArguments(t);
        try { 
            return wrapNullString(mth.invoke(obj, parameters));
        } catch (Exception x) { 
            x.printStackTrace();
            throw new Error("Method invocation error");
        }
    }
    
    Object evaluateObj(FilterIterator t) {
        Object obj = getTarget(t);
        Object[] parameters = evaluateArguments(t);
        try { 
            return mth.invoke(obj, parameters);
        } catch (Exception x) { 
            x.printStackTrace();
            throw new Error("Method invocation error");
        }
    }    

    InvokeNode(Node target, Method mth, Node arguments[]) { 
        super(getFieldType(mth.getReturnType()), opInvoke);
        this.target = target;
        this.arguments = arguments;
        this.mth = mth;
    }
}


class InvokeAnyNode extends Node { 
    Node     target;
    Node[]   arguments;
    Class[]  profile;
    String   methodName;
    String   containsFieldName;

    public boolean equals(Object o) { 
        if (!(o instanceof InvokeAnyNode)) { 
            return false;
        }
        InvokeAnyNode node = (InvokeAnyNode)o;        
        return equalObjects(node.target, target)
            && Arrays.equals(node.arguments, arguments)
            && Arrays.equals(node.profile, profile)
            && equalObjects(node.methodName, methodName)
            && equalObjects(node.containsFieldName, containsFieldName);
    }

    Class getType() { 
        return Object.class;
    }

    String getFieldName() { 
        if (target != null) {         
            if (target.tag != opCurrent) { 
                String baseName = target.getFieldName();
                return (baseName != null) ? baseName + "." + methodName : null;
            } else { 
                return methodName;
            }
        } else { 
            return containsFieldName != null ? containsFieldName + "." + methodName : methodName;
        }
    }

    InvokeAnyNode(Node target, String name, Node arguments[], String containsFieldName) { 
        super(tpAny, opInvokeAny);
        this.target = target;
        this.containsFieldName = containsFieldName;
        methodName = name;
        this.arguments = arguments;
        profile = new Class[arguments.length];
    }

    Object evaluateObj(FilterIterator t) {
        Class cls;
        Method m;
        Object obj = t.currObj;
        if (target != null) { 
            obj = target.evaluateObj(t);
            if (obj == null) { 
                throw new JSQLNullPointerException(null, methodName);
            }
        }
        Object[] parameters = null;
        int n = arguments.length; 
        if (n > 0) { 
            parameters = new Object[n];
            for (int i = 0; i < n; i++) { 
                Node arg = arguments[i];
                Object value;
                Class type;
                switch (arg.type) { 
                case Node.tpInt:
                    value = new Long(arg.evaluateInt(t));
                    type = long.class;
                    break;
                case Node.tpReal:
                    value = new Double(arg.evaluateReal(t)); 
                    type = double.class;
                    break;
                case Node.tpStr:
                    value = arg.evaluateStr(t);
                    type = String.class;
                    break;
                case Node.tpDate:
                    value = arg.evaluateDate(t);
                    type = Date.class;
                    break;
                case Node.tpBool:
                    value = new Boolean(arg.evaluateBool(t));
                    type = boolean.class;
                    break;
                default:
                    value = arg.evaluateObj(t);
                    if (value != null) {
                        type = value.getClass();
                        if (type.equals(Long.class) || type.equals(Integer.class) || type.equals(Byte.class)
                            || type.equals(Character.class) || type.equals(Short.class))
                        { 
                            type = long.class;
                        } else if (type.equals(Float.class) || type.equals(Double.class)) { 
                            type = double.class;
                        } else if (type.equals(Boolean.class)) { 
                            type = boolean.class;
                        }
                    } else { 
                        type = Object.class;
                    }
                }
                parameters[i] = value;
                profile[i] = type;
            }
        }
        try { 
            if (target == null && t.containsElem != null) { 
                if ((m = QueryImpl.lookupMethod(t.containsElem.getClass(), methodName, profile)) != null) {
                    return t.query.resolve(m.invoke(t.containsElem, parameters));
                }
            }
            cls = obj.getClass();
            if ((m = QueryImpl.lookupMethod(cls, methodName, profile)) != null) { 
                return t.query.resolve(m.invoke(t.containsElem, parameters));
            } 
        } catch (InvocationTargetException x) {
            x.printStackTrace();
            throw new IllegalAccessError();            
        } catch(IllegalAccessException x) { 
            x.printStackTrace();
            throw new IllegalAccessError();
        }

        throw new JSQLNoSuchFieldException(cls, methodName);
    }
}        


class ConvertAnyNode extends Node { 
    public boolean equals(Object o) { 
        return o instanceof ConvertAnyNode && super.equals(o) && ((ConvertAnyNode)o).expr.equals(expr);
    }

    boolean evaluateBool(FilterIterator t) { 
        return ((Boolean)evaluateObj(t)).booleanValue();
    }

    long evaluateInt(FilterIterator t) { 
        return ((Number)evaluateObj(t)).longValue();
    }

    double evaluateReal(FilterIterator t) { 
        return ((Number)evaluateObj(t)).doubleValue();
    }
    
    Object evaluateObj(FilterIterator t) { 
        return expr.evaluateObj(t);
    }

    ConvertAnyNode(int type, Node expr) { 
        super(type, opConvertAny);
        this.expr = expr;
    }
    Node expr;
}

class BinOpNode extends Node { 
    Node left;
    Node right;

    public boolean equals(Object o) { 
        return o instanceof BinOpNode 
            && super.equals(o)
            && ((BinOpNode)o).left.equals(left) 
            && ((BinOpNode)o).right.equals(right);
    }

    long evaluateInt(FilterIterator t) {
        long lval = left.evaluateInt(t);
        long rval = right.evaluateInt(t);
        long res;
        switch (tag) { 
          case opIntAdd:
            return lval + rval;
          case opIntSub:
            return lval - rval;
          case opIntMul:
            return lval * rval;
          case opIntDiv:
            if (rval == 0) { 
                throw new JSQLArithmeticException("Divided by zero");
            }
            return lval / rval;
          case opIntAnd:
            return lval & rval;
          case opIntOr:
            return lval | rval;
          case opIntPow:
            res = 1;
            if (rval < 0) {
                lval = 1/lval;
                rval = -rval;
            }
            while (rval != 0) {
                if ((rval & 1) != 0) { 
                    res *= lval;
                }
                lval *= lval;
                rval >>>= 1;
            }
            return res; 
          default:
            throw new Error("Invalid tag");
        }
    }

    double evaluateReal(FilterIterator t) {
        double lval = left.evaluateReal(t);
        double rval = right.evaluateReal(t);
        switch (tag) { 
          case opRealAdd:
            return lval + rval;
          case opRealSub:
            return lval - rval;
          case opRealMul:
            return lval * rval;
          case opRealDiv:
            return lval / rval;
          case opRealPow:
            return Math.pow(lval, rval);
          default:
            throw new Error("Invalid tag");
        }
    }

    String evaluateStr(FilterIterator t) {
        String lval = left.evaluateStr(t);
        String rval = right.evaluateStr(t);
        return lval + rval;
    }

    Object evaluateObj(FilterIterator t) { 
        Object lval, rval;
        try { 
            lval = left.evaluateObj(t);
        } catch (JSQLRuntimeException x) { 
            t.query.reportRuntimeError(x);
            rval = right.evaluateObj(t);
            if (rval instanceof Boolean) { 
                return ((Boolean)rval).booleanValue()? Boolean.TRUE : Boolean.FALSE;
            }
            throw x;
        }
            
        if (lval instanceof Boolean) { 
            switch (tag) { 
              case opAnyAnd:
                return ((Boolean)lval).booleanValue() && ((Boolean)right.evaluateObj(t)).booleanValue()
                    ? Boolean.TRUE : Boolean.FALSE;
              case opAnyOr:
                return ((Boolean)lval).booleanValue() || ((Boolean)right.evaluateObj(t)).booleanValue()
                    ? Boolean.TRUE : Boolean.FALSE;
              default:
                throw new Error("Operation is not applicable to operands of boolean type");
            }
        }
        rval = right.evaluateObj(t);
        if (lval instanceof Double || lval instanceof Float 
            || rval instanceof Double || rval instanceof Float)
        {
            double r1 = ((Number)lval).doubleValue();
            double r2 = ((Number)rval).doubleValue();
            switch (tag) {
              case opAnyAdd:
                return new Double(r1 + r2);
              case opAnySub:
                return new Double(r1 - r2);
              case opAnyMul:
                return new Double(r1 * r2);
              case opAnyDiv:
                return new Double(r1 / r2);
              case opAnyPow:
                return new Double(Math.pow(r1, r2));
              default:
                throw new Error("Operation is not applicable to operands of real type");
            }
        } else if (lval instanceof String && rval instanceof String) { 
            return (String)lval + (String)rval;
        } else { 
            long i1 = ((Number)lval).longValue();
            long i2 = ((Number)rval).longValue();
            long res;
            switch (tag) { 
              case opAnyAdd:
                return new Long(i1 + i2);
              case opAnySub:
                return new Long(i1 - i2);
              case opAnyMul:
                return new Long(i1 * i2);
              case opAnyDiv:
                if (i2 == 0) { 
                    throw new JSQLArithmeticException("Divided by zero");
                }
                return new Long(i1 / i2);
              case opAnyAnd:
                return new Long(i1 & i2);
              case opAnyOr:
                return new Long(i1 | i2);
              case opAnyPow:
                res = 1;
                if (i1 < 0) {
                    i2 = 1/i2;
                    i1 = -i1;
                }
                while (i1 != 0) {
                    if ((i1 & 1) != 0) { 
                        res *= i2;
                    }
                    i2 *= i2;
                    i1 >>>= 1;
                }
                return new Long(res);             
              default:
                throw new Error("Operation is not applicable to operands of integer type");
            }
        }
    }

    static boolean areEqual(Object a, Object b) { 
        if (a == b) {
            return true;
        }
        if (a instanceof Double || a instanceof Float || b instanceof Double || b instanceof Float) { 
            return ((Number)a).doubleValue() == ((Number)b).doubleValue();
        } else if (a instanceof Number || b instanceof Number) { 
            return ((Number)a).longValue() == ((Number)b).longValue();
        } else if (a != null) { 
            return a.equals(b);
        }
        return false;
    }

    static int compare(Object a, Object b) { 
        if (a == null) { 
            return b == null ? 0 : -1;
        } else if (b == null) { 
            return 1;
        } else if (a instanceof Double || a instanceof Float || b instanceof Double || b instanceof Float) { 
            double r1 = ((Number)a).doubleValue();
            double r2 = ((Number)b).doubleValue();
            return r1 < r2 ? -1 : r1 == r2 ? 0 : 1;
        } else if (a instanceof Number || b instanceof Number) { 
            long i1 = ((Number)a).longValue();
            long i2 = ((Number)b).longValue();
            return i1 < i2 ? -1 : i1 == i2 ? 0 : 1; 
        } else { 
            return ((Comparable)a).compareTo(b);
        }
    }

    boolean evaluateBool(FilterIterator t) {
        switch (tag) { 
          case opAnyEq:
            return areEqual(left.evaluateObj(t), right.evaluateObj(t));
          case opAnyNe:
            return !areEqual(left.evaluateObj(t), right.evaluateObj(t));
          case opAnyLt:
            return compare(left.evaluateObj(t), right.evaluateObj(t)) < 0;
          case opAnyLe:
            return compare(left.evaluateObj(t), right.evaluateObj(t)) <= 0;
          case opAnyGt:
            return compare(left.evaluateObj(t), right.evaluateObj(t)) > 0;
          case opAnyGe:
            return compare(left.evaluateObj(t), right.evaluateObj(t)) >= 0;
          case opInAny:
          {
              Object elem = left.evaluateObj(t);
              Object set =  right.evaluateObj(t);
              if (set instanceof String) {
                  return ((String)set).indexOf((String)elem) >= 0;
              } else {  
                  Object[] arr = (Object[])set;
                  for (int i = arr.length; --i >= 0;) { 
                      if (elem.equals(arr[i])) { 
                          return true;
                      }
                  }
                  return false;
              }
          }
          case opBoolAnd:
            try { 
                if (!left.evaluateBool(t)) { 
                    return false;
                }
            } catch (JSQLRuntimeException x) { 
                t.query.reportRuntimeError(x);
            }
            return right.evaluateBool(t);
          case opBoolOr:
            try { 
                if (left.evaluateBool(t)) { 
                    return true;
                }
            } catch (JSQLRuntimeException x) { 
                t.query.reportRuntimeError(x);
            }
            return right.evaluateBool(t);

          case opIntEq:
            return left.evaluateInt(t) == right.evaluateInt(t);
          case opIntNe:
            return left.evaluateInt(t) != right.evaluateInt(t);
          case opIntLt:
            return left.evaluateInt(t) < right.evaluateInt(t);
          case opIntLe:
            return left.evaluateInt(t) <= right.evaluateInt(t);
          case opIntGt:
            return left.evaluateInt(t) > right.evaluateInt(t);
          case opIntGe:
            return left.evaluateInt(t) >= right.evaluateInt(t);

          case opRealEq:
            return left.evaluateReal(t) == right.evaluateReal(t);
          case opRealNe:
            return left.evaluateReal(t) != right.evaluateReal(t);
          case opRealLt:
            return left.evaluateReal(t) <  right.evaluateReal(t);
          case opRealLe:
            return left.evaluateReal(t) <= right.evaluateReal(t);
          case opRealGt:
            return left.evaluateReal(t) >  right.evaluateReal(t);
          case opRealGe:
            return left.evaluateReal(t) >= right.evaluateReal(t);

          case opStrEq:
            return left.evaluateStr(t).equals(right.evaluateStr(t));
          case opStrNe:
            return !left.evaluateStr(t).equals(right.evaluateStr(t));
          case opStrLt:
            return left.evaluateStr(t).compareTo(right.evaluateStr(t)) < 0;
          case opStrLe:
            return left.evaluateStr(t).compareTo(right.evaluateStr(t)) <= 0;
          case opStrGt:
            return left.evaluateStr(t).compareTo(right.evaluateStr(t)) > 0;
          case opStrGe:
            return left.evaluateStr(t).compareTo(right.evaluateStr(t)) >= 0;

          case opDateEq: 
            return left.evaluateDate(t).equals(right.evaluateDate(t));			
          case opDateNe: 
            return !left.evaluateDate(t).equals(right.evaluateDate(t));            
          case opDateLt: 
            return left.evaluateDate(t).compareTo(right.evaluateDate(t)) < 0;          
          case opDateLe: 
            return left.evaluateDate(t).compareTo(right.evaluateDate(t)) <= 0;          
          case opDateGt: 
            return left.evaluateDate(t).compareTo(right.evaluateDate(t)) > 0;          
          case opDateGe: 
            return left.evaluateDate(t).compareTo(right.evaluateDate(t)) >= 0;
				
          case opBoolEq:
            return left.evaluateBool(t) == right.evaluateBool(t);
          case opBoolNe:
            return left.evaluateBool(t) != right.evaluateBool(t);

          case opObjEq:
            return areEqual(left.evaluateObj(t), right.evaluateObj(t));
          case opObjNe:
            return !areEqual(left.evaluateObj(t), right.evaluateObj(t));

          case opScanCollection:
            return ((Collection)right.evaluateObj(t)).contains(left.evaluateObj(t));
          case opScanArrayBool:
          {
            boolean val = left.evaluateBool(t);
            boolean[] arr = (boolean[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayInt1:
          {
            long val = left.evaluateInt(t);
            byte[] arr = (byte[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayChar:
          {
            long val = left.evaluateInt(t);
            char[] arr = (char[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayInt2:
          {
            long val = left.evaluateInt(t);
            short[] arr = (short[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayInt4:
          {
            long val = left.evaluateInt(t);
            int[] arr = (int[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayInt8:
          {
            long val = left.evaluateInt(t);
            long[] arr = (long[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayReal4:
          {
            double val = left.evaluateReal(t);
            float[] arr = (float[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayReal8:
          {
            double val = left.evaluateReal(t);
            double[] arr = (double[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (arr[i] == val) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayStr:
          {
            String val = left.evaluateStr(t);
            String[] arr = (String[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (val.equals(arr[i])) { 
                    return true;
                }
            }
            return false;
          }
          case opScanArrayObj:
          {
            Object val = left.evaluateObj(t);
            Object[] arr = (Object[])right.evaluateObj(t);  
            for (int i = arr.length; --i >= 0;) { 
                if (areEqual(val, arr[i])) { 
                    return true;
                }
            }
            return false;
          }
          case opInString:
          {
            String substr = left.evaluateStr(t);
            String str = right.evaluateStr(t);
            return str.indexOf(substr) >= 0;
          }
          default:
            throw new Error("Invalid tag " + tag);
        }
    }

    BinOpNode(int type, int tag, Node left, Node right) {
        super(type, tag);
        this.left = left;
        this.right = right;
    }
}

class CompareNode extends Node {
    Node o1, o2, o3;

    public boolean equals(Object o) { 
        return o instanceof CompareNode
            && super.equals(o) 
            && ((CompareNode)o).o1.equals(o1) && ((CompareNode)o).o2.equals(o2)
            && equalObjects(((CompareNode)o).o3, o3);
    }

    boolean evaluateBool(FilterIterator t) {
        switch(tag) { 
          case opAnyBetween:
          {
              Object val = o1.evaluateObj(t);
              return BinOpNode.compare(val, o2.evaluateObj(t)) >= 0 && BinOpNode.compare(val, o3.evaluateObj(t)) <= 0;
          }
          case opIntBetween:
          {
            long val = o1.evaluateInt(t);
            return val >= o2.evaluateInt(t) && val <= o3.evaluateInt(t);
          }       
          case opRealBetween:
          {
            double val = o1.evaluateReal(t);
            return val >= o2.evaluateReal(t) && val <= o3.evaluateReal(t);
          }
          case opStrBetween:
          {
            String val = o1.evaluateStr(t);
            return val.compareTo(o2.evaluateStr(t)) >= 0 
                && val.compareTo(o3.evaluateStr(t)) <= 0;
          }
          case opDateBetween:
          {
            Date val = o1.evaluateDate(t);
            return val.compareTo(o2.evaluateDate(t)) >= 0 
                && val.compareTo(o3.evaluateDate(t)) <= 0;
          }
          case opStrLike:
          {
            String str = o1.evaluateStr(t);
            String pat = o2.evaluateStr(t);
            int pi = 0, si = 0, pn = pat.length(), sn = str.length(); 
            int wildcard = -1, strpos = -1;
            while (true) { 
                if (pi < pn && pat.charAt(pi) == '%') { 
                    wildcard = ++pi;
                    strpos = si;
                } else if (si == sn) { 
                    return pi == pn;
                } else if (pi < pn && (str.charAt(si) == pat.charAt(pi) 
                                       || pat.charAt(pi) == '_')) {
                    si += 1;
                    pi += 1;
                } else if (wildcard >= 0) { 
                    si = ++strpos;
                    pi = wildcard;
                } else { 
                    return false;
                }
            }
          }
          case opStrLikeEsc:
          {
            String str = o1.evaluateStr(t);
            String pat = o2.evaluateStr(t);
            char escape = o3.evaluateStr(t).charAt(0);
            int pi = 0, si = 0, pn = pat.length(), sn = str.length(); 
            int wildcard = -1, strpos = -1;
            while (true) { 
                if (pi < pn && pat.charAt(pi) == '%') { 
                    wildcard = ++pi;
                    strpos = si;
                } else if (si == sn) { 
                    return pi == pn;
                } else if (pi+1 < pn && pat.charAt(pi) == escape && 
                           pat.charAt(pi+1) == str.charAt(si)) {
                    si += 1;
                    pi += 2;
                } else if (pi < pn && ((pat.charAt(pi) != escape 
                                        && (str.charAt(si) == pat.charAt(pi) 
                                            || pat.charAt(pi) == '_')))) {
                    si += 1;
                    pi += 1;
                } else if (wildcard >= 0) { 
                    si = ++strpos;
                    pi = wildcard;
                } else { 
                    return false;
                }
            }
          }
          default:
            throw new Error("Invalid tag " + tag);
        }
    }

    CompareNode(int tag, Node a, Node b, Node c) { 
        super(tpBool, tag);
        o1 = a;
        o2 = b;
        o3 = c;
    }
}


class UnaryOpNode extends Node { 
    Node opd;

    public boolean equals(Object o) { 
        return o instanceof UnaryOpNode && super.equals(o) && ((UnaryOpNode)o).opd.equals(opd);
    }

    Object evaluateObj(FilterIterator t) { 
        Object val = opd.evaluateObj(t);
        switch (tag) {
          case opAnyNeg:
            return val instanceof Double || val instanceof Float 
                ? (Object)new Double(-((Number)val).doubleValue())
                : (Object)new Long(-((Number)val).longValue());
          case opAnyAbs:
            if (val instanceof Double || val instanceof Float) { 
                double rval = ((Number)val).doubleValue();
                return new Double(rval < 0 ? -rval : rval);
            } else { 
                long ival = ((Number)val).longValue();
                return new Long(ival < 0 ? -ival : ival);
            }
          case opAnyNot:
            return val instanceof Boolean 
                ? ((Boolean)val).booleanValue() ? Boolean.FALSE : Boolean.TRUE
                : (Object)new Long(~((Number)val).longValue());
          default:
            throw new Error("Invalid tag " + tag);
        } 
    }

    long evaluateInt(FilterIterator t) {
        long val;
        switch (tag) {
          case opIntNot:
            return ~opd.evaluateInt(t);
          case opIntNeg:
            return -opd.evaluateInt(t);
          case opIntAbs:
            val = opd.evaluateInt(t);
            return val < 0 ? -val : val;
          case opRealToInt:
            return (long)opd.evaluateReal(t);
          case opAnyLength:
            { 
                Object obj = opd.evaluateObj(t);
                if (obj instanceof String) { 
                    return ((String)obj).length();
                } else { 
                    try { 
                        return Array.getLength(obj);
                    } catch (IllegalArgumentException x) {
                        throw new Error("Argument is not array");
                    }
                }                  
            }
          case opLength:
            try { 
                return Array.getLength(opd.evaluateObj(t));
            } catch (IllegalArgumentException x) {
                throw new Error("Argument is not array");
            }
          case opStrLength:
            return opd.evaluateStr(t).length();
          default:
            throw new Error("Invalid tag " + tag);
        }
    }
    
    double evaluateReal(FilterIterator t) { 
        double val;
        switch (tag) { 
          case opRealNeg:
            return -opd.evaluateReal(t);
          case opRealAbs:
            val = opd.evaluateReal(t);
            return val < 0 ? -val : val;
          case opRealSin:
            return Math.sin(opd.evaluateReal(t));
          case opRealCos:
            return Math.cos(opd.evaluateReal(t));
          case opRealTan:
            return Math.tan(opd.evaluateReal(t));
          case opRealAsin:
            return Math.asin(opd.evaluateReal(t));
          case opRealAcos:
            return Math.acos(opd.evaluateReal(t));
          case opRealAtan:
            return Math.atan(opd.evaluateReal(t));
          case opRealExp:
            return Math.exp(opd.evaluateReal(t));
          case opRealLog:
            return Math.log(opd.evaluateReal(t));
          case opRealSqrt:
            return Math.sqrt(opd.evaluateReal(t));
          case opRealCeil:
            return Math.ceil(opd.evaluateReal(t));
          case opRealFloor:
            return Math.floor(opd.evaluateReal(t));
          case opIntToReal:
            return (double)opd.evaluateInt(t);
          default:
            throw new Error("Invalid tag " + tag);
        }
    }
    
    Date evaluateDate(FilterIterator t) { 
        switch (tag) { 
          case opStrToDate:
            return QueryImpl.parseDate(opd.evaluateStr(t));
          default:
            throw new Error("Invalid tag " + tag);
        }
    }              

    String evaluateStr(FilterIterator t) { 
        switch (tag) { 
          case opStrUpper:
            return opd.evaluateStr(t).toUpperCase();
          case opStrLower:
            return opd.evaluateStr(t).toLowerCase();
          case opIntToStr:
            return Long.toString(opd.evaluateInt(t), 10);
          case opRealToStr:
            return Double.toString(opd.evaluateReal(t));
          case opDateToStr:
              return opd.evaluateDate(t).toString();
          case opAnyToStr:
            return opd.evaluateObj(t).toString();
          default:
            throw new Error("Invalid tag " + tag);
        }
    }
    
    boolean evaluateBool(FilterIterator t) { 
        switch (tag) { 
          case opBoolNot:
            return !opd.evaluateBool(t);
          case opIsNull:
            return opd.evaluateObj(t) == null;
          default:
            throw new Error("Invalid tag " + tag);
        }
    }

    UnaryOpNode(int type, int tag, Node node) { 
        super(type, tag);
        opd = node;
    }
}


class LoadAnyNode extends Node { 
    String fieldName;
    String containsFieldName;
    Node   base;
    Field  f;
    Method m;
    
    public boolean equals(Object o) { 
        if (!(o instanceof LoadAnyNode)) { 
            return false;
        }
        LoadAnyNode node = (LoadAnyNode)o;        
        return equalObjects(node.base, base)
            && equalObjects(node.fieldName, fieldName)
            && equalObjects(node.f, f)
            && equalObjects(node.m, m);
    }

    Class getType() { 
        return Object.class;
    }

    String getFieldName() { 
        if (base != null) { 
            if (base.tag != opCurrent) { 
                String baseName = base.getFieldName();
                return (baseName != null) ? baseName + "." + fieldName : null;
            } else { 
                return fieldName;
            }
        } else { 
            return containsFieldName != null ? containsFieldName + "." + fieldName : fieldName;
        }
    }

    LoadAnyNode(Node base, String name, String containsFieldName) { 
        super(tpAny, opLoadAny);
        fieldName = name;
        this.containsFieldName = containsFieldName;
        this.base = base;
    }

    public String toString() { 
        return "LoadAnyNode: fieldName='" + fieldName + "', containsFieldName='" 
            + containsFieldName + "', base=(" + base + "), f=" + f + ", m=" + m;
    }
    

    Object evaluateObj(FilterIterator t) { 
        Object obj;
        Class  cls;
        Field f = this.f;
        Method m  = this.m;                    
        try { 
            if (base == null) { 
                if (t.containsElem != null) { 
                    obj = t.containsElem;
                    cls = obj.getClass();
                    if (f != null && f.getDeclaringClass().equals(cls)) { 
                        return t.query.resolve(f.get(obj));
                    }
                    if (m != null && m.getDeclaringClass().equals(cls)) { 
                        return t.query.resolve(m.invoke(obj));
                    }
                    if ((f = ClassDescriptor.locateField(cls, fieldName)) != null) { 
                        this.f = f;
                        return t.query.resolve(f.get(obj));
                    }
                    if ((m = QueryImpl.lookupMethod(cls, fieldName, QueryImpl.defaultProfile)) != null) {
                        this.m = m;
                        return t.query.resolve(m.invoke(obj));
                    }
                }
                obj = t.currObj;
            } else { 
                obj = base.evaluateObj(t);
                if (obj == null) { 
                    throw new JSQLNullPointerException(null, fieldName);
                }
            }
            cls = obj.getClass();
            if (f != null && f.getDeclaringClass().equals(cls)) { 
                return t.query.resolve(f.get(obj));
            }
            if (m != null && m.getDeclaringClass().equals(cls)) { 
                return t.query.resolve(m.invoke(obj));
            }
            if ((f = ClassDescriptor.locateField(cls, fieldName)) != null) { 
                this.f = f;
                return t.query.resolve(f.get(obj));
            }
            if ((m = QueryImpl.lookupMethod(cls, fieldName, QueryImpl.defaultProfile)) != null) {
                this.m = m;
                return t.query.resolve(m.invoke(obj));
            }
        } catch(IllegalAccessException x) { 
            x.printStackTrace();
            throw new IllegalAccessError();
        } catch (InvocationTargetException x) {
            x.printStackTrace();
            throw new IllegalAccessError();            
        }

        throw new JSQLNoSuchFieldException(cls, fieldName);
    }
}
    
class ResolveNode extends Node { 
    Resolver resolver;
    Class    resolvedClass;
    Node     expr;

    public boolean equals(Object o) { 
        return o instanceof ResolveNode 
            && ((ResolveNode)o).expr.equals(expr) 
            && ((ResolveNode)o).resolver.equals(resolver)
            && ((ResolveNode)o).resolvedClass.equals(resolvedClass);
    }

    Class getType() { 
        return resolvedClass;
    }

    Object evaluateObj(FilterIterator t) { 
        return resolver.resolve(expr.evaluateObj(t));
    }
    
    String getFieldName() { 
        if (expr != null) { 
            return expr.getFieldName(); 
        } else { 
            return null; 
        } 
    }

    ResolveNode(Node expr, Resolver resolver, Class resolvedClass) { 
        super(tpObj, opResolve);
        this.expr = expr;
        this.resolver = resolver;
        this.resolvedClass = resolvedClass;
    }
}

class LoadNode extends Node {
    Field field;
    Node  base;

    public boolean equals(Object o) { 
        return o instanceof LoadNode 
            && super.equals(o)
            && ((LoadNode)o).field.equals(field) 
            && equalObjects(((LoadNode)o).base, base);
    }

    Class getType() { 
        return field.getType();
    }

    String getFieldName() { 
        if (base != null && base.tag != opCurrent) { 
            String baseName = base.getFieldName();
            return (baseName != null) ? baseName + "." + field.getName() : null;
        } else { 
            return field.getName();
        }
    }

    final Object getBase(FilterIterator t) { 
        if (base == null) {
            return t.currObj;
        }        
        Object obj = base.evaluateObj(t);
        if (obj == null) {
            throw new JSQLNullPointerException(base.getType(), field.getName());
        }
        return obj;
    }

    long evaluateInt(FilterIterator t) {
        try { 
            return field.getLong(getBase(t));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    
    double evaluateReal(FilterIterator t) { 
        try { 
            return field.getDouble(getBase(t));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    
    boolean evaluateBool(FilterIterator t) { 
        try { 
            return field.getBoolean(getBase(t));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
        
    String evaluateStr(FilterIterator t) { 
        try {
            return wrapNullString(field.get(getBase(t)));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
        
    Object evaluateObj(FilterIterator t) { 
        try {
            return field.get(getBase(t));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    
    LoadNode(Node base, Field f) { 
        super(getFieldType(f.getType()), opLoad);
        field = f;
        this.base = base;
    }
}


class AggregateFunctionNode extends Node { 
    public boolean equals(Object o) { 
        return o instanceof AggregateFunctionNode
            && super.equals(o)
            && equalObjects(((AggregateFunctionNode)o).argument, argument)
            && ((AggregateFunctionNode)o).index == index;
    }

    long    evaluateInt(FilterIterator t) {
        return t.intAggragateFuncValue[index];
    }

    double  evaluateReal(FilterIterator t) {
        return t.realAggragateFuncValue[index];
    }

    AggregateFunctionNode(int type, int tag, Node arg) {
        super(type, tag);
        argument = arg;
    }

    int     index;
    Node    argument;
}

class InvokeElementNode extends InvokeNode { 
    String containsArrayName;

    InvokeElementNode(Method mth, Node arguments[], String arrayName) { 
        super(null, mth, arguments);
        containsArrayName = arrayName;
    }

    public boolean equals(Object o) { 
        return o instanceof InvokeElementNode && super.equals(o);
    }

    Object getTarget(FilterIterator t) { 
        return t.containsElem;
    }

    String getFieldName() { 
        if (containsArrayName != null) { 
            return containsArrayName + "." + mth.getName();
        } else { 
            return null;
        }
    }
}

class ElementNode extends Node { 
    String arrayName;
    Field  field;
    Class  type;

    public boolean equals(Object o) { 
        return o instanceof ElementNode 
            && equalObjects(((ElementNode)o).arrayName, arrayName)
            && equalObjects(((ElementNode)o).field, field)
            && equalObjects(((ElementNode)o).type, type);
    }

    ElementNode(String array, Field f) { 
        super(getFieldType(f.getType()), opElement);
        arrayName = array;
        type = f.getType();
        field = f;
    }

    String getFieldName() { 
        return arrayName != null ? arrayName + "." + field.getName() : null;
    }
    
    boolean evaluateBool(FilterIterator t) { 
        try { 
            return field.getBoolean(t.containsElem);
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    long    evaluateInt(FilterIterator t) {
        try { 
            return field.getLong(t.containsElem);
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    double  evaluateReal(FilterIterator t) {
        try { 
            return field.getDouble(t.containsElem);
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    String  evaluateStr(FilterIterator t) {
        try { 
            return wrapNullString(field.get(t.containsElem));
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    Object  evaluateObj(FilterIterator t) {
        try { 
            return field.get(t.containsElem);
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }
    Class   getType() { 
        return type;
    }

}

class ContainsNode extends Node implements Comparator { 
    Node      containsExpr;
    Field     groupByField;
    Method    groupByMethod;
    String    groupByFieldName;
    Class     containsFieldClass;
    int       groupByType;
    Node      havingExpr;
    Node      withExpr;
    Resolver  resolver;
    ArrayList aggregateFunctions;

    public boolean equals(Object o) { 
        if (!(o instanceof ContainsNode)) { 
            return false;
        }
        ContainsNode node = (ContainsNode)o;
        return node.containsExpr.equals(containsExpr)
            && equalObjects(node.groupByField, groupByField)
            && equalObjects(node.groupByMethod, groupByMethod)
            && equalObjects(node.groupByFieldName, groupByFieldName)
            && equalObjects(node.containsFieldClass, containsFieldClass)
            && node.groupByType == groupByType
            && equalObjects(node.havingExpr, havingExpr)
            && equalObjects(node.withExpr, withExpr)
            && equalObjects(node.aggregateFunctions, aggregateFunctions);
    }

    public int compare(Object o1, Object o2) {
        if (o1 == o2) { 
            return 0;
        }
        try {
            if (groupByMethod != null) { 
                return ((Comparable)groupByMethod.invoke(o1)).compareTo(groupByMethod.invoke(o2));
            } 
            switch (groupByType) { 
              case tpInt:
                {
                    long v1 = groupByField.getLong(o1);
                    long v2 = groupByField.getLong(o2);
                    return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
                }
              case tpReal:
                {
                    double v1 = groupByField.getDouble(o1);
                    double v2 = groupByField.getDouble(o2);
                    return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
                }
              case tpBool:
                {
                    boolean v1 = groupByField.getBoolean(o1);
                    boolean v2 = groupByField.getBoolean(o2);
                    return v1 ? (v2 ? 0 : 1) : (v2 ? -1 : 0);
                }
              default:
                return ((Comparable)groupByField.get(o1)).compareTo(groupByField.get(o2));
            }
        } catch (InvocationTargetException x) {
            x.printStackTrace();
            throw new IllegalAccessError();            
        } catch(IllegalAccessException x) { 
            x.printStackTrace();
            throw new IllegalAccessError();
        }
    }


    boolean evaluateBool(FilterIterator t) { 
        int i, j, k, l, n = 0, len = 0;
        Object   collection;
        collection = containsExpr.evaluateObj(t);
        if (collection == null) {
            return false;
        }
        Object[] sortedArray = null;        
        if (havingExpr != null && (withExpr != null || !(collection instanceof Collection))) { 
            n = (collection instanceof Collection) 
                ? ((Collection)collection).size() : ((Object[])collection).length;
            if (t.containsArray == null || t.containsArray.length < n) { 
                t.containsArray = new Object[n];
            }
            sortedArray = t.containsArray;
            t.containsArray = null; // prevent reuse of the same array by nexted CONTAINS in with expression
        }
        Object saveContainsElem = t.containsElem;

        if (collection instanceof Collection) { 
            if (withExpr != null) { 
                Object elem;
                Iterator iterator = ((Collection)collection).iterator();
                while (iterator.hasNext()) { 
                    elem = iterator.next();
                    if (elem != null) { 
                        if (resolver != null) { 
                            elem = resolver.resolve(elem);
                        } else { 
                            elem = t.query.resolve(elem);
                        }
                        t.containsElem = elem;
                        try { 
                            if (withExpr.evaluateBool(t)) { 
                                if (havingExpr == null) {
                                    t.containsElem = saveContainsElem;
                                    return true;
                                }
                                sortedArray[len++] = elem;
                            }
                        } catch (JSQLRuntimeException x) {
                            t.query.reportRuntimeError(x);
                        }
                    }
                }
            } else { 
                sortedArray = ((Collection)collection).toArray();
                n = sortedArray.length;
                if (t.query.resolveMap != null) { 
                    for (i = 0; i < n; i++) { 
                        sortedArray[i] = t.query.resolve(sortedArray[i]);
                    }
                }
                len = n;
            }
        } else {
            Object[] a = (Object[])collection;
            n = a.length;
            if (withExpr != null) { 
                for (i = 0; i < n; i++) { 
                    Object elem = a[i];
                    if (elem != null) { 
                        if (resolver != null) { 
                            elem = resolver.resolve(elem);
                        } else { 
                            elem = t.query.resolve(elem);
                        }
                        t.containsElem = elem;
                        try { 
                            if (withExpr.evaluateBool(t)) { 
                                if (havingExpr == null) {
                                    t.containsElem = saveContainsElem;
                                    return true;
                                }
                                sortedArray[len++] = elem;
                            }
                        } catch (JSQLRuntimeException x) {
                            t.query.reportRuntimeError(x);
                        }
                    }
                }
            } else { 
                System.arraycopy(a, 0, sortedArray, 0, n);
                if (t.query.resolveMap != null) { 
                    for (i = 0; i < n; i++) { 
                        sortedArray[i] = t.query.resolve(sortedArray[i]);
                    }
                }
                len = n;
            }
        }
        t.containsElem = saveContainsElem;
        if (sortedArray != null) { 
            t.containsArray = sortedArray;
        }
        if (len == 0) {  
            return false;
        }
        if (groupByFieldName != null && len > 0) {
            Class type = sortedArray[0].getClass();
            groupByField = ClassDescriptor.locateField(type, groupByFieldName);
            if (groupByField == null) { 
                groupByMethod = QueryImpl.lookupMethod(type, groupByFieldName, QueryImpl.defaultProfile);
                if (groupByMethod == null) { 
                    throw new JSQLNoSuchFieldException(type, groupByFieldName);
                }
            } else { 
                groupByType = Node.getFieldType(groupByField.getType());
            }
        }
        Arrays.sort(sortedArray, 0, len, this);

        n = aggregateFunctions.size();
        if (t.intAggragateFuncValue == null || t.intAggragateFuncValue.length < n) { 
            t.intAggragateFuncValue = new long[n];
            t.realAggragateFuncValue = new double[n];
        }
        for (i = 0; i < len; i = j) {             
            for (j = i+1; j < len && compare(sortedArray[i], sortedArray[j]) == 0; j++);
            for (k = 0; k < n; k++) {
                AggregateFunctionNode agr = (AggregateFunctionNode)aggregateFunctions.get(k);
                Node argument = agr.argument;
                if (agr.type == tpInt) { 
                    long ival = 0;
                    switch (agr.tag) { 
                      case opSum:
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            ival += argument.evaluateInt(t);
                        }
                        break;
                      case opAvg:
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            ival += argument.evaluateInt(t);
                        }
                        ival /= j - i;
                        break;
                      case opMin:
                        ival = Long.MAX_VALUE;
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            long v = argument.evaluateInt(t);
                            if (v < ival) { 
                                ival = v;
                            }
                        }
                        break;
                      case opMax:
                        ival = Long.MIN_VALUE;
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            long v = argument.evaluateInt(t);
                            if (v > ival) { 
                                ival = v;
                            }
                        }
                        break;
                      case opCount:
                        ival = j - i;
                    }
                    t.intAggragateFuncValue[k] = ival;
                } else {
                    double rval = 0.0;

                    switch (agr.tag) { 
                      case opSum:
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            rval += argument.evaluateReal(t);
                        }
                        break;
                      case opAvg:
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            rval += argument.evaluateReal(t);
                        }
                        rval /= j - i;
                        break;
                      case opMin:
                        rval = Double.POSITIVE_INFINITY;
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            double v = argument.evaluateReal(t);
                            if (v < rval) { 
                                rval = v;
                            }
                        }
                        break;
                      case opMax:
                        rval = Double.NEGATIVE_INFINITY;
                        for (l = i; l < j; l++) { 
                            t.containsElem = sortedArray[l];
                            double v = argument.evaluateReal(t);
                            if (v > rval) { 
                                rval = v;
                            }
                        }
                    }
                    t.realAggragateFuncValue[k] = rval;
                }
            }
            t.containsElem = saveContainsElem;
            try { 
                if (havingExpr.evaluateBool(t)) { 
                    return true;
                }
            } catch (JSQLRuntimeException x) {
                t.query.reportRuntimeError(x);
            }
        }
        return false;
    }


    ContainsNode(Node containsExpr, Class containsFieldClass) { 
        super(tpBool, opContains);
        this.containsExpr = containsExpr;
        this.containsFieldClass = containsFieldClass;
        aggregateFunctions = new ArrayList();
    }
}


class OrderNode {
    OrderNode next;
    boolean   ascent;
    Field     field;
    Method    method;
    String    fieldName;
    int       type;
    
    final int compare(Object a, Object b) { 
        int diff;
        try {
            if (method != null) {
                diff = ((Comparable)method.invoke(a)).compareTo(method.invoke(b));
            } else { 
                switch (type) { 
                  case ClassDescriptor.tpBoolean:
                    diff = field.getBoolean(a) ? field.getBoolean(b) ? 0 : 1 
                        : field.getBoolean(b) ? -1 : 0;
                    break;
                  case ClassDescriptor.tpChar:
                    diff = field.getChar(a) - field.getChar(b);
                    break;
                  case ClassDescriptor.tpByte:
                    diff = field.getByte(a) - field.getByte(b);
                    break;
                  case ClassDescriptor.tpShort:
                    diff = field.getShort(a) - field.getShort(b);
                    break;
                  case ClassDescriptor.tpInt:
                    {
                        int l = field.getInt(a);
                        int r = field.getInt(b);
                        diff = l < r ? -1 : l == r ? 0 : 1;
                        break;
                    }
                  case ClassDescriptor.tpLong:
                    {
                        long l = field.getLong(a);
                        long r = field.getLong(b);
                        diff = l < r ? -1 : l == r ? 0 : 1;
                        break;
                    }
                  case ClassDescriptor.tpFloat:
                    {
                        float l = field.getFloat(a);
                        float r = field.getFloat(b);
                        diff = l < r ? -1 : l == r ? 0 : 1;
                        break;
                    }
                  case ClassDescriptor.tpDouble:
                    {
                        double l = field.getDouble(a);
                        double r = field.getDouble(b);
                        diff = l < r ? -1 : l == r ? 0 : 1;
                        break;
                    }
                  default:
                    diff = ((Comparable)field.get(a)).compareTo(field.get(b));
                    break;
                }
            }
        } catch(IllegalAccessException x) { 
            x.printStackTrace();
            throw new IllegalAccessError();
        } catch (InvocationTargetException x) {
            x.printStackTrace();
            throw new IllegalAccessError();            
        }
        if (diff == 0 && next != null) { 
            return next.compare(a, b);
        }
        if (!ascent) { 
            diff = -diff;
        }
        return diff;
    }

    void resolveName(Class cls) { 
        field = ClassDescriptor.locateField(cls, fieldName);
        if (field == null) { 
            method = QueryImpl.lookupMethod(cls, fieldName, QueryImpl.defaultProfile);
            if (method == null) { 
                throw new JSQLNoSuchFieldException(cls, fieldName);
            }
        }
    }

    OrderNode(int type, Field field) { 
        this.type = type;
        this.field = field;
        ascent = true;
    }
    OrderNode(Method method) { 
        this.method = method;
        ascent = true;
    }
    OrderNode(String name) { 
        fieldName = name;
        ascent = true;
    }
}

class ParameterNode extends LiteralNode {
    ArrayList params;
    int       index;

    public boolean equals(Object o) { 
        return o instanceof ParameterNode && ((ParameterNode)o).index == index;
    }
    
    boolean evaluateBool(FilterIterator t) { 
        return ((Boolean)params.get(index)).booleanValue();
    }
    long    evaluateInt(FilterIterator t) {
        return ((Number)params.get(index)).longValue();
    }
    double  evaluateReal(FilterIterator t) {
        return ((Number)params.get(index)).doubleValue();
    }
    String  evaluateStr(FilterIterator t) {
        return (String)params.get(index);
    }
    Date    evaluateDate(FilterIterator t) {
        return (Date)params.get(index);
    }

    Object getValue() {
        return params.get(index);
    }

    ParameterNode(ArrayList parameterList) { 
        super(tpUnknown, opParameter);
        params = parameterList;
        index = params.size();
        params.add(null);
    }
}


class Symbol { 
    int tkn;

    Symbol(int tkn) { 
        this.tkn = tkn;
    }
}


class Binding {
    Binding next;
    String  name;
    boolean used;
    int     loopId;

    Binding(String ident, int loop, Binding chain) { 
        name = ident;
        used = false;
        next = chain;
        loopId = loop;
    }
}

public class QueryImpl<T> implements Query<T> 
{
    public IterableIterator<T> select(Class cls, Iterator<T> iterator, String query) throws CompileError
    {
        this.query = query;
        buf = query.toCharArray();
        str = new char[buf.length];
        this.cls = cls;

        compile();
        return execute(iterator);
    }

    public IterableIterator<T> select(String className, Iterator<T> iterator, String query) throws CompileError

    {
        cls = ClassDescriptor.loadClass(storage, className);
        return select(cls, iterator, query);
    }

    public void setParameter(int index, Object value)
    {
        parameters.set(index-1, value);
    }

    public void setIntParameter(int index, long value)
    {
        setParameter(index, new Long(value));
    }

    public void setRealParameter(int index, double value)
    {
        setParameter(index, new Double(value));
    }

    public void setBoolParameter(int index, boolean value)
    {
        setParameter(index, new Boolean(value));
    }

    public void prepare(Class cls, String query)
    {
        this.query = query;
        buf = query.toCharArray();
        str = new char[buf.length];
        this.cls = cls;
        compile();
    }

    public void prepare(String className, String query)
    {
        cls = ClassDescriptor.loadClass(storage, className);
        this.query = query;
        buf = query.toCharArray();
        str = new char[buf.length];
        compile();
    }

    public IterableIterator<T> execute(Iterator<T> iterator)
    {       
        IterableIterator<T> result = (IterableIterator<T>)applyIndex(tree);
        if (result == null) { 
            if (storage.listener != null) { 
                storage.listener.sequentialSearchPerformed(query);
            }
            result = new FilterIterator<T>(this, iterator, tree);
        }
        if (order != null) {
            ArrayList<T> list = new ArrayList<T>();
            while (result.hasNext()) { 
                list.add(result.next());
            }
            sort(list);
            return new IteratorWrapper<T>(list.iterator());
        }
        return result;
    }
            
    private void sort(ArrayList<T> selection) { 
        int i, j, k, n;
        OrderNode order = this.order;
        T top;

        if (selection.size() == 0) {
            return;
        }
        for (OrderNode ord = order; ord != null; ord = ord.next) {             
            if (ord.fieldName != null) { 
                ord.resolveName(selection.get(0).getClass());
            }
        }

        for (n = selection.size(), i = n/2, j = i; i >= 1; i--) { 
            k = i;
            top = selection.get(k-1);
            do { 
                if (k*2 == n || 
                    order.compare(selection.get(k*2-1), selection.get(k*2)) > 0) 
                { 
                    if (order.compare(top, selection.get(k*2-1)) >= 0) {
                        break;
                    }
                    selection.set(k-1, selection.get(k*2-1));
                    k = k*2;
                } else { 
                    if (order.compare(top, selection.get(k*2)) >= 0) {
                        break;
                    }
                    selection.set(k-1, selection.get(k*2));
                    k = k*2+1;
                }
            } while (k <= j);
            selection.set(k-1, top); 
        }
        for (i = n; i >= 2; i--) { 
            top = selection.get(i-1);
            selection.set(i-1, selection.get(0));
            selection.set(0, top);
            for (k = 1, j = (i-1)/2; k <= j;) { 
                if (k*2 == i-1 || 
                    order.compare(selection.get(k*2-1), selection.get(k*2)) > 0) 
                { 
                    if (order.compare(top, selection.get(k*2-1)) >= 0) {
                        break;
                    }
                    selection.set(k-1, selection.get(k*2-1));
                    k = k*2;
                } else { 
                    if (order.compare(top, selection.get(k*2)) >= 0) {
                        break;
                    }
                    selection.set(k-1, selection.get(k*2));
                    k = k*2+1;
                }
            } 
            selection.set(k-1, top);
        } 
    }

    public void reportRuntimeError(JSQLRuntimeException x) { 
        if (runtimeErrorsReporting) { 
            StringBuffer buf = new StringBuffer();
            buf.append(x.getMessage());
            Class cls = x.getTarget();
            if (cls != null) { 
                buf.append(cls.getName());
                buf.append('.');
            }
            String fieldName = x.getFieldName();
            if (fieldName != null) {
                buf.append(fieldName);
            }
            System.err.println(buf);
        }
        if (storage != null && storage.listener != null) { 
            storage.listener.JSQLRuntimeError(x);
        }
    }

    public void enableRuntimeErrorReporting(boolean enabled) { 
        runtimeErrorsReporting = enabled;
    }
    
    static class ResolveMapping { 
        Class    resolved;
        Resolver resolver;

        ResolveMapping(Class resolved, Resolver resolver) { 
            this.resolved = resolved;
            this.resolver = resolver;
        }
    };

    public void setResolver(Class original, Class resolved, Resolver resolver) {
        if (resolveMap == null) { 
            resolveMap = new HashMap();
        }
        resolveMap.put(original, new ResolveMapping(resolved, resolver));
    }

    public void addIndex(String key, GenericIndex<T> index) { 
        if (indices == null) { 
            indices = new HashMap();
        }
        indices.put(key, index);
    }

    private final GenericIndex<T> getIndex(String key) { 
        return indices != null ? (GenericIndex)indices.get(key) : null;
    }
        
    private static Key keyLiteral(Class type, Node node, boolean inclusive) {
        Object value = ((LiteralNode)node).getValue();
        if (type.equals(long.class)) { 
            return new Key(((Number)value).longValue(), inclusive);
        } else if (type.equals(int.class)) { 
            return new Key(((Number)value).intValue(), inclusive);
        } else if (type.equals(byte.class)) { 
            return new Key(((Number)value).byteValue(), inclusive);
        } else if (type.equals(short.class)) { 
            return new Key(((Number)value).shortValue(), inclusive);
        } else if (type.equals(char.class)) { 
            return new Key(value instanceof Number 
                           ? (char)((Number)value).intValue()
                           : ((Character)value).charValue(), inclusive);
        } else if (type.equals(float.class)) {            
            return new Key(((Number)value).floatValue(), inclusive);
        } else if (type.equals(double.class)) {       
            return new Key(((Number)value).doubleValue(), inclusive);                
        } else if (type.equals(String.class)) {  
            return new Key((String)value, inclusive);     
        } else if (type.equals(Date.class)) {  
            return new Key((Date)value, inclusive);     
        }
        return null;
    }

    public QueryImpl(Storage storage) {
        this.storage = (StorageImpl)storage;
        parameters = new ArrayList();
        runtimeErrorsReporting = true;
    }

    int           pos;
    char[]        buf;
    char[]        str;
    String        query;
    long          ivalue;
    String        svalue;
    double        fvalue;
    Class         cls;
    Node          tree;
    String        ident;
    int           lex;
    int           vars;
    Binding       bindings;
    OrderNode     order;
    ContainsNode  contains;
    ArrayList     parameters;
    boolean       runtimeErrorsReporting;
    HashMap       resolveMap;

    HashMap<String,GenericIndex<T>> indices;
    StorageImpl   storage;

    static DateFormat dateFormat;
    static Hashtable symtab;
    static Class[] defaultProfile = new Class[0];
    static Node[]  noArguments = new Node[0];
    
    final static Object dummyKeyValue = new Object();

    final static int tknIdent = 1;
    final static int tknLpar = 2;
    final static int tknRpar = 3;
    final static int tknLbr = 4;
    final static int tknRbr = 5;
    final static int tknDot = 6;
    final static int tknComma = 7;
    final static int tknPower = 8;
    final static int tknIconst = 9;
    final static int tknSconst = 10;
    final static int tknFconst = 11;
    final static int tknAdd = 12;
    final static int tknSub = 13;
    final static int tknMul = 14;
    final static int tknDiv = 15;
    final static int tknAnd = 16;
    final static int tknOr = 17;
    final static int tknNot = 18;
    final static int tknNull = 19;
    final static int tknNeg = 20;
    final static int tknEq = 21;
    final static int tknNe = 22;
    final static int tknGt = 23;
    final static int tknGe = 24;
    final static int tknLt = 25;
    final static int tknLe = 26;
    final static int tknBetween = 27;
    final static int tknEscape = 28;
    final static int tknExists = 29;
    final static int tknLike = 30;
    final static int tknIn = 31;
    final static int tknLength = 32;
    final static int tknLower = 33;
    final static int tknUpper = 34;
    final static int tknAbs = 35;
    final static int tknIs = 36;
    final static int tknInteger = 37;
    final static int tknReal = 38;
    final static int tknString = 39;
    final static int tknFirst = 40;
    final static int tknLast = 41;
    final static int tknCurrent = 42;
    final static int tknCol = 44;
    final static int tknTrue = 45;
    final static int tknFalse = 46;
    final static int tknWhere = 47;
    final static int tknOrder = 48;
    final static int tknAsc = 49;
    final static int tknDesc = 50;
    final static int tknEof = 51;
    final static int tknSin = 52;
    final static int tknCos = 53;
    final static int tknTan = 54;
    final static int tknAsin = 55;
    final static int tknAcos = 56;
    final static int tknAtan = 57;
    final static int tknSqrt = 58;
    final static int tknLog = 59;
    final static int tknExp = 60;
    final static int tknCeil = 61;
    final static int tknFloor = 62;
    final static int tknBy = 63;
    final static int tknHaving = 64;
    final static int tknGroup = 65;
    final static int tknAvg = 66;
    final static int tknCount = 67;
    final static int tknMax = 68;
    final static int tknMin = 69;
    final static int tknSum = 70;
    final static int tknWith = 71;
    final static int tknParam = 72;
    final static int tknContains = 73;

    static { 
        symtab = new Hashtable();
        symtab.put("abs", new Symbol(tknAbs));
        symtab.put("acos", new Symbol(tknAcos));
        symtab.put("and", new Symbol(tknAnd));
        symtab.put("asc", new Symbol(tknAsc));
        symtab.put("asin", new Symbol(tknAsin));
        symtab.put("atan", new Symbol(tknAtan));
        symtab.put("between", new Symbol(tknBetween));
        symtab.put("by", new Symbol(tknBy));
        symtab.put("ceal", new Symbol(tknCeil));
        symtab.put("cos", new Symbol(tknCos));
        symtab.put("current", new Symbol(tknCurrent));
        symtab.put("desc", new Symbol(tknDesc));
        symtab.put("escape", new Symbol(tknEscape));
        symtab.put("exists", new Symbol(tknExists));
        symtab.put("exp", new Symbol(tknExp));
        symtab.put("false", new Symbol(tknFalse));
        symtab.put("floor", new Symbol(tknFloor));
        symtab.put("in", new Symbol(tknIn));
        symtab.put("is", new Symbol(tknIs));
        symtab.put("integer", new Symbol(tknInteger));
        symtab.put("last", new Symbol(tknLast));
        symtab.put("length", new Symbol(tknLength));
        symtab.put("like", new Symbol(tknLike));
        symtab.put("log", new Symbol(tknLog));
        symtab.put("lower", new Symbol(tknLower));
        symtab.put("not", new Symbol(tknNot));
        symtab.put("null", new Symbol(tknNull));
        symtab.put("or", new Symbol(tknOr));
        symtab.put("order", new Symbol(tknOrder));
        symtab.put("real", new Symbol(tknReal));
        symtab.put("sin", new Symbol(tknSin));
        symtab.put("sqrt", new Symbol(tknSqrt));
        symtab.put("string", new Symbol(tknString));
        symtab.put("true", new Symbol(tknTrue));
        symtab.put("upper", new Symbol(tknUpper));
        symtab.put("having", new Symbol(tknHaving));
        symtab.put("contains", new Symbol(tknContains));
        symtab.put("group", new Symbol(tknGroup));
        symtab.put("min", new Symbol(tknMin));
        symtab.put("max", new Symbol(tknMax));
        symtab.put("count", new Symbol(tknCount));
        symtab.put("avg", new Symbol(tknAvg));
        symtab.put("sum", new Symbol(tknSum));
        symtab.put("with", new Symbol(tknWith));
        dateFormat = DateFormat.getTimeInstance();
    }

    static Date parseDate(String source) { 
       ParsePosition pos = new ParsePosition(0);
       Date result;
       synchronized (dateFormat) { 
           result = dateFormat.parse(source, pos);
       } 
       if (pos.getIndex() == 0) { 
           throw new Error("Parse error for date \"" + source + "\" at position " + pos.getErrorIndex());
       }
       return result;
    }          

    final int scan() {
        int p = pos;
        int eol = buf.length;
        char ch = 0;
        int i;
        while (p < eol && Character.isWhitespace(ch = buf[p])) { 
            p += 1;
        }
        if (p == eol) { 
            return tknEof;
        }
        pos = ++p;
        switch (ch) { 
          case '+':
            return tknAdd;
          case '-':
            return tknSub;
          case '*':
            return tknMul;
          case '/':
            return tknDiv;
          case '.':
            return tknDot;
          case ',':
            return tknComma;
          case '(':
            return tknLpar;
          case ')':
            return tknRpar;
          case '[':
            return tknLbr;
          case ']':
            return tknRbr;
          case ':':
            return tknCol;
          case '^':
            return tknPower;
          case '?':
            return tknParam;
          case '<':
            if (p < eol) { 
                if (buf[p] == '=') { 
                    pos += 1;
                    return tknLe;
                } 
                if (buf[p] == '>') { 
                    pos += 1;
                    return tknNe;
                }
            }
            return tknLt;
          case '>':
            if (p < eol && buf[p] == '=') { 
                pos += 1;
                return tknGe;
            } 
            return tknGt;
          case '=':
            return tknEq;
          case '!':
            if (p == eol || buf[p] != '=') { 
                throw new CompileError("Invalid token '!'", p-1);
            } 
            pos += 1;
            return tknNe;
          case '|':
            if (p == eol || buf[p] != '|') { 
                throw new CompileError("Invalid token '!'", p-1);
            } 
            pos += 1;
            return tknAdd;
          case '\'':
            i = 0; 
            while (true) { 
                if (p == eol) { 
                    throw new CompileError("Unexpected end of string constant",
                                           p);
                }
                if (buf[p] == '\'') { 
                    if (++p == eol || buf[p] != '\'') { 
                        svalue = new String(str, 0, i);
                        pos = p;
                        return tknSconst;
                    }
                }
                str[i++] = buf[p++];
            }
          case '0': case '1': case '2': case '3': case '4': 
          case '5': case '6': case '7': case '8': case '9':
            i = p - 1;
            while (p < eol && Character.isDigit(ch = buf[p])) { 
                p += 1;
            }
            if (ch == '.' || ch == 'e' || ch == 'E') { 
                while (++p < eol && (Character.isDigit(buf[p]) 
                       || buf[p] == 'e' || buf[p] == 'E' || buf[p] == '.' ||
                       ((ch == 'e' || ch == 'E') 
                        && (buf[p] == '-' || buf[p] == '+'))));
                pos = p;
                try { 
                    fvalue = 
                        Double.valueOf(query.substring(i, p)).doubleValue();
                } catch(NumberFormatException x) { 
                    throw new CompileError("Bad floating point constant", i);
                }
                return tknFconst;
            } else { 
                pos = p;
                try { 
                    ivalue = Long.parseLong(query.substring(i, p), 10);
                } catch(NumberFormatException x) { 
                    throw new CompileError("Bad floating point constant", i);
                }
                return tknIconst;
            }
          default:
            if (Character.isLetter(ch) || ch == '$' || ch == '_') { 
                i = p-1;
                while (p < eol && (Character.isLetterOrDigit(ch = buf[p]) 
                                   || ch == '$' || ch == '_'))
                {
                    p += 1;
                }
                pos = p;
                ident = query.substring(i, p);
                Symbol s = (Symbol)symtab.get(ident);
                return (s == null) ? tknIdent : s.tkn;
            } else { 
                throw new CompileError("Invalid symbol: " + ch, p-1);
            }
        }
    }
        

    final Node disjunction() {
        Node left = conjunction();
        if (lex == tknOr) { 
            int p = pos;
            Node right = disjunction();
            if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                left = new BinOpNode(Node.tpInt, Node.opIntOr, left, right);
            } else if (left.type == Node.tpBool && right.type == Node.tpBool) {
                left = new BinOpNode(Node.tpBool, Node.opBoolOr, left, right);
            } else if (left.type == Node.tpAny || right.type == Node.tpAny) { 
                left = new BinOpNode(Node.tpAny, Node.opAnyOr, left, right);               
            } else { 
                throw new CompileError("Bad operands for OR operator", p);
            }
        }
        return left;
    }

    final Node conjunction() {
        Node left = comparison();
        if (lex == tknAnd) { 
            int p = pos;
            Node right = conjunction();
            if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                left = new BinOpNode(Node.tpInt, Node.opIntAnd, left, right);
            } else if (left.type == Node.tpBool && right.type == Node.tpBool) {
                left = new BinOpNode(Node.tpBool, Node.opBoolAnd, left, right);
            } else if (left.type == Node.tpAny || right.type == Node.tpAny) { 
                left = new BinOpNode(Node.tpAny, Node.opAnyAnd, left, right);               
            } else { 
                throw new CompileError("Bad operands for AND operator", p);
            }
        }
        return left;
    }

    final static Node int2real(Node expr) {
        if (expr.tag == Node.opIntConst) { 
            return new RealLiteralNode((double)((IntLiteralNode)expr).value);
        } 
        return new UnaryOpNode(Node.tpReal, Node.opIntToReal, expr);
    }

    final static Node str2date(Node expr) {
        if (expr.tag == Node.opStrConst)  {
            return new DateLiteralNode(parseDate(((StrLiteralNode)expr).value));
        }
        return new UnaryOpNode(Node.tpDate, Node.opStrToDate, expr);
    }
		
    final int compare(Node expr, BinOpNode list)
    {
        int n = 1;
        if (list.left != null) { 
            n = compare(expr, (BinOpNode)list.left);
        }
        Node elem = list.right;
        int cop = Node.opNop;
        if (elem.type == Node.tpUnknown) { 
            elem.type = expr.type;
        }
        if (expr.type == Node.tpInt) { 
            if (elem.type == Node.tpReal) { 
                expr = new UnaryOpNode(Node.tpReal, Node.opIntToReal, expr);
                cop = Node.opRealEq;
            } else if (elem.type == Node.tpInt) { 
                cop = Node.opIntEq;
            }
        } else if (expr.type == Node.tpReal) {
            if (elem.type == Node.tpReal) { 
                cop = Node.opRealEq;
            } else if (elem.type == Node.tpInt) { 
                cop = Node.opRealEq;
                elem = int2real(elem);
            }
        } else if (expr.type == Node.tpDate && elem.type == Node.tpDate) {
            cop = Node.opDateEq;
        } else if (expr.type == Node.tpStr && elem.type == Node.tpStr) {
            cop = Node.opStrEq;
        } else if (expr.type == Node.tpObj && elem.type == Node.tpObj) {
            cop = Node.opObjEq;
        } else if (expr.type == Node.tpBool && elem.type == Node.tpBool) {
            cop = Node.opBoolEq;
        } else if (expr.type == Node.tpAny) { 
            cop = Node.opAnyEq;
        }
        if (cop == Node.opNop) { 
            throw new CompileError("Expression "+n+" in right part of IN "+
                                   "operator has incompatible type", pos);
        } 
        list.type = Node.tpBool;
        if (list.left != null) { 
            list.right = new BinOpNode(Node.tpBool, cop, expr, elem);
            list.tag = Node.opBoolOr;
        } else { 
            list.left = expr;
            list.right = elem;
            list.tag = cop;
        }
        return ++n;
    }


    final Node comparison() {
        int  leftPos = pos;
        Node left, right;
        left = addition();
        int cop = lex;
        if (cop == tknEq || cop == tknNe || cop == tknGt || cop == tknGe
            || cop == tknLe || cop == tknLt || cop == tknBetween 
            || cop == tknLike || cop == tknNot || cop == tknIs || cop == tknIn)
        {
            int rightPos = pos;
            boolean not = false;
            if (cop == tknNot) { 
                not = true;
                cop = scan();
                if (cop != tknLike && cop != tknBetween && cop != tknIn) { 
                    throw new CompileError("LIKE, BETWEEN or IN expected", 
                                           rightPos);
                } 
                rightPos = pos;
            } else if (cop == tknIs) {
                if (left.type < Node.tpObj) { 
                    throw new CompileError("IS [NOT] NULL predicate can be applied only to references,arrays or string", 
                                           rightPos);
                } 
                rightPos = pos;
                if ((cop = scan()) == tknNull) { 
                    left = new UnaryOpNode(Node.tpBool, Node.opIsNull, left);
                } else if (cop == tknNot) { 
                    rightPos = pos;
                    if (scan() == tknNull) { 
                        left = new UnaryOpNode(Node.tpBool, Node.opBoolNot, 
                                               new UnaryOpNode(Node.tpBool, 
                                                               Node.opIsNull, 
                                                               left));
                    } else { 
                        throw new CompileError("NULL expected", rightPos);
                    }
                } else { 
                    throw new CompileError("[NOT] NULL expected", rightPos);
                } 
                lex = scan();
                return left;
            }   
            right = addition();
            if (cop == tknIn) { 
                int type;
                if (right.type != Node.tpList && (left.type == Node.tpAny || right.type == Node.tpAny)) { 
                    left = new BinOpNode(Node.tpBool, Node.opInAny, left, right);
                } else {                     
                    switch (right.type) {
                      case Node.tpCollection:
                        left = new BinOpNode(Node.tpBool, Node.opScanCollection,
                                             left, right);
                        break;
                      case Node.tpArrayBool:
                        if (left.type != Node.tpBool) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayBool,
                                             left, right);
                        break;
                      case Node.tpArrayInt1:
                        if (left.type != Node.tpInt) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayInt1,
                                             left, right);
                        break;
                      case Node.tpArrayChar:
                        if (left.type != Node.tpInt) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayChar,
                                         left, right);
                        break;
                      case Node.tpArrayInt2:
                        if (left.type != Node.tpInt) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayInt2,
                                             left, right);
                        break;
                      case Node.tpArrayInt4:
                        if (left.type != Node.tpInt) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayInt4,
                                             left, right);
                        break;
                      case Node.tpArrayInt8:
                        if (left.type != Node.tpInt) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayInt8,
                                             left, right);
                        break;
                      case Node.tpArrayReal4:
                        if (left.type == Node.tpInt) {
                            left = int2real(left);
                        } else if (left.type != Node.tpReal) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayReal4,
                                             left, right);
                        break;
                      case Node.tpArrayReal8:
                        if (left.type == Node.tpInt) {
                            left = int2real(left);
                        } else if (left.type != Node.tpReal) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayReal8,
                                             left, right);
                        break;
                      case Node.tpArrayObj:
                        if (left.type != Node.tpObj) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayObj,
                                             left, right);
                        break;
                      case Node.tpArrayStr:
                        if (left.type != Node.tpStr) { 
                            throw new CompileError("Incompatible types of IN operator operands", 
                                                   rightPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opScanArrayStr,
                                             left, right);
                        break;
                      case Node.tpStr:
                        if (left.type != Node.tpStr) { 
                            throw new CompileError("Left operand of IN expression hasn't string type",
                                                   leftPos);
                        }
                        left = new BinOpNode(Node.tpBool, Node.opInString,
                                             left, right);
                        break;
                      case Node.tpList:
                        compare(left, (BinOpNode)right);
                        left = right;
                        break;
                      default:
                        throw new CompileError("List of expressions or array expected", 
                                               rightPos);
                    }
                }
            } else if (cop == tknBetween) { 
                int andPos = pos;
                if (lex != tknAnd) { 
                    throw new CompileError("AND expected", pos);
                }
                Node right2 = addition();
                if (right.type == Node.tpUnknown) { 
                    right.type = left.type;
                }
                if (right2.type == Node.tpUnknown) { 
                    right2.type = left.type;
                }
                if (left.type == Node.tpAny || right.type == Node.tpAny || right2.type == Node.tpAny) { 
                    left = new CompareNode(Node.opAnyBetween, left, right, right2);                    
                } else if (left.type == Node.tpReal || right.type == Node.tpReal || right2.type == Node.tpReal) {
                    if (left.type == Node.tpInt) { 
                        left = int2real(left);
                    } else if (left.type != Node.tpReal) { 
                        throw new CompileError("operand of BETWEEN operator should be of integer, real or string type", 
                                               leftPos);
                    }
                    if (right.type == Node.tpInt) {
                        right = int2real(right);
                    } else if (right.type != Node.tpReal) { 
                        throw new CompileError("operand of BETWEEN operator should be of integer, real or string type", 
                                               rightPos);
                    }
                    if (right2.type == Node.tpInt) {
                        right2 = int2real(right2);
                    } else if (right2.type != Node.tpReal) { 
                        throw new CompileError("operand of BETWEEN operator should be of integer, real or string type", 
                                               andPos);
                    }
                    left = new CompareNode(Node.opRealBetween, 
                                           left, right, right2);
                } 
                else if (left.type == Node.tpInt 
                         && right.type == Node.tpInt 
                         && right2.type == Node.tpInt)
                {                   
                    left = new CompareNode(Node.opIntBetween, 
                                           left, right, right2);
                }
                else if (left.type == Node.tpStr && right.type == Node.tpStr 
                         && right2.type == Node.tpStr)
                {
                    left = new CompareNode(Node.opStrBetween, 
                                           left, right, right2);
                }
                else if (left.type == Node.tpDate) { 
                    if (right.type == Node.tpStr) {
                        right = str2date(right);
                    } else if (right.type != Node.tpDate) {
                        throw new CompileError("operands of BETWEEN operator should be of date type", rightPos);
                    }
                    if (right2.type == Node.tpStr) {
                        right2 = str2date(right2);
                    } else if (right2.type != Node.tpDate) {
                        throw new CompileError("operands of BETWEEN operator should be of date type", andPos);
                    }
                    left = new CompareNode(Node.opDateBetween, left, right, right2);
                } else { 
                    throw new CompileError("operands of BETWEEN operator should be of integer, real or string type", 
                                           rightPos);
                }
            } else if (cop == tknLike) {  
                if (right.type == Node.tpUnknown) { 
                    right.type = left.type;
                }
                if (left.type == Node.tpAny) { 
                    left = new ConvertAnyNode(Node.tpStr, left);
                }
                if (right.type == Node.tpAny) { 
                    right = new ConvertAnyNode(Node.tpStr, right);
                }
                if (left.type != Node.tpStr || right.type != Node.tpStr) { 
                    throw new CompileError("operands of LIKE operator should be of string type", 
                                           rightPos);
                }
                if (lex == tknEscape) { 
                    rightPos = pos;
                    if (scan() != tknSconst) { 
                        throw new CompileError("String literal espected after ESCAPE", rightPos);
                    }
                    left = new CompareNode(Node.opStrLikeEsc, 
                                           left, right,
                                           new StrLiteralNode(svalue));
                    lex = scan();
                } else { 
                    left = new CompareNode(Node.opStrLike, left, right, null);
                }
            } else { 
                if (right.type == Node.tpUnknown) { 
                    right.type = left.type;
                }
                if (left.type == Node.tpUnknown) { 
                    left.type = right.type;
                }
                if (left.type == Node.tpAny || right.type == Node.tpAny) {
                    left = new BinOpNode(Node.tpBool, Node.opAnyEq+cop-tknEq, 
                                         left, right);                    
                } else if (left.type == Node.tpReal || right.type == Node.tpReal) { 
                    if (left.type == Node.tpInt) { 
                        left = int2real(left);
                    } else if (left.type != Node.tpReal) { 
                        throw new CompileError("operands of relation operator should be of intger, real or string type", 
                                               leftPos);
                    }
                    if (right.type == Node.tpInt) { 
                        right = int2real(right);
                    } else if (right.type != Node.tpReal) { 
                        throw new CompileError("operands of relation operator should be of intger, real or string type", 
                                               rightPos);
                    }
                    left = new BinOpNode(Node.tpBool, Node.opRealEq+cop-tknEq, 
                                         left, right);
                } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                    left = new BinOpNode(Node.tpBool, Node.opIntEq+cop-tknEq, 
                                         left, right);
                } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
                    left = new BinOpNode(Node.tpBool, Node.opStrEq+cop-tknEq, 
                                         left, right);
                } else if (left.type == Node.tpDate) {
                    if (right.type == Node.tpStr) {
                        right = str2date(right);
                    } else if (right.type != Node.tpDate) {
                        throw new CompileError("right opeerand of relation operator should be of date type", rightPos);
                    }
                    left = new BinOpNode(Node.tpBool, Node.opDateEq + cop - tknEq, left, right);
                } else if (left.type == Node.tpObj && right.type == Node.tpObj) { 
                    if (cop != tknEq && cop != tknNe) { 
                        throw new CompileError("References can be checked only for equality", 
                                               rightPos);
                    }
                    left = new BinOpNode(Node.tpBool, Node.opObjEq+cop-tknEq, 
                                         left, right);
                } else if (left.type == Node.tpBool && right.type == Node.tpBool) { 
                    if (cop != tknEq && cop != tknNe) { 
                        throw new CompileError("Boolean variables can be checked only for equality",
                                               rightPos);
                    }
                    left = new BinOpNode(Node.tpBool, Node.opBoolEq+cop-tknEq, 
                                         left, right);
                } else { 
                    throw new CompileError("operands of relation operator should be of integer, real or string type", 
                                           rightPos);
                }
            }
            if (not) { 
                left = new UnaryOpNode(Node.tpBool, Node.opBoolNot, left);
            }
        }
        return left;
    }


    final Node addition() { 
        int leftPos = pos;
        Node left = multiplication();
        while (lex == tknAdd || lex == tknSub) { 
            int cop = lex;
            int rightPos = pos;
            Node right = multiplication();
            if (left.type == Node.tpAny || right.type == Node.tpAny) { 
                left = new BinOpNode(Node.tpAny, cop == tknAdd ? Node.opAnyAdd : Node.opAnySub,
                                     left, right);                
            } else if (left.type == Node.tpReal || right.type == Node.tpReal) { 
                if (left.type == Node.tpInt) { 
                    left = int2real(left);
                } else if (left.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operators should be of integer or real type", 
                                           leftPos);
                }
                if (right.type == Node.tpInt) { 
                    right = int2real(right);
                } else if (right.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operator should be of integer or real type", 
                                           rightPos);
                }
                left = new BinOpNode(Node.tpReal, cop == tknAdd 
                                     ? Node.opRealAdd : Node.opRealSub,
                                     left, right);
            } 
            else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                left = new BinOpNode(Node.tpInt, cop == tknAdd 
                                     ? Node.opIntAdd : Node.opIntSub,
                                     left, right);
            } else if (left.type == Node.tpStr && right.type == Node.tpStr) { 
                if (cop == tknAdd) { 
                    left = new BinOpNode(Node.tpStr, Node.opStrConcat, 
                                         left, right);
                } else { 
                    throw new CompileError("Operation - is not defined for strings", 
                                           rightPos);
                }
            } else { 
                throw new CompileError("operands of arithmentic operator should be of integer or real type", 
                                       rightPos);
            }
            leftPos = rightPos;
        }
        return left;
    }


    final Node multiplication() { 
        int leftPos = pos;
        Node left = power();
        while (lex == tknMul || lex == tknDiv) { 
            int cop = lex;
            int rightPos = pos;
            Node right = power();
            if (left.type == Node.tpAny || right.type == Node.tpAny) { 
                left = new BinOpNode(Node.tpAny, cop == tknMul ? Node.opAnyMul : Node.opAnyDiv, 
                                     left, right);                
            } else if (left.type == Node.tpReal || right.type == Node.tpReal) { 
                if (left.type == Node.tpInt) { 
                    left = int2real(left);
                } else if (left.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operators should be of integer or real type", 
                                           leftPos);
                }
                if (right.type == Node.tpInt) { 
                    right = int2real(right);
                } else if (right.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operator should be of integer or real type", 
                                           rightPos);
                }
                left = new BinOpNode(Node.tpReal, cop == tknMul 
                                     ? Node.opRealMul : Node.opRealDiv,
                                     left, right);
            } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                left = new BinOpNode(Node.tpInt, cop == tknMul 
                                     ? Node.opIntMul : Node.opIntDiv,
                                     left, right);
            } else { 
                throw new CompileError("operands of arithmentic operator should be of integer or real type", 
                                       rightPos);
            }
            leftPos = rightPos;
        }
        return left;
    }


    final Node power() { 
        int leftPos = pos;
        Node left = term();
        if (lex == tknPower) { 
            int rightPos = pos;
            Node right = power();
            if (left.type == Node.tpAny || right.type == Node.tpAny) { 
                left = new BinOpNode(Node.tpAny, Node.opAnyPow, left, right);
            } else if (left.type == Node.tpReal || right.type == Node.tpReal) { 
                if (left.type == Node.tpInt) { 
                    left = int2real(left);
                } else if (left.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operators should be of integer or real type", 
                                           leftPos);
                }
                if (right.type == Node.tpInt) { 
                    right = int2real(right);
                } else if (right.type != Node.tpReal) { 
                    throw new CompileError("operands of arithmetic operator should be of integer or real type", 
                                           rightPos);
                }
                left = new BinOpNode(Node.tpReal, Node.opRealPow, left, right);
            } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
                left = new BinOpNode(Node.tpInt, Node.opIntPow, left, right);
            } else { 
                throw new CompileError("operands of arithmentic operator should be of integer or real type", 
                                       rightPos);
            }
        }
        return left;
    }

    static Method lookupMethod(Class cls, String ident, Class[] profile) 
    { 
        Method m = null;
        for (Class scope = cls; scope != null; scope = scope.getSuperclass()) { 
            try { 
                m = scope.getDeclaredMethod(ident, profile); 
                break;
            } catch(Exception x) {} 
        }        
        if (m == null && profile.length == 0) { 
            ident = "get" + Character.toUpperCase(ident.charAt(0)) + ident.substring(1);
            for (Class scope = cls; scope != null; scope = scope.getSuperclass()) { 
                try { 
                    m = scope.getDeclaredMethod(ident, profile); 
                    break;
                } catch(Exception x) {} 
            }
        }           
        if (m != null) { 
            try { 
                m.setAccessible(true);
            } catch(Exception x) {}
        }  
        return m;
    }

    final Object resolve(Object obj) { 
        if (resolveMap != null) { 
            ResolveMapping rm = (ResolveMapping)resolveMap.get(obj.getClass());
            if (rm != null) { 
                obj = rm.resolver.resolve(obj);
            }
        }
        return obj;
    }
        

    final Node component(Node base, Class cls) { 
        String c;
        String ident = this.ident;
        Field f;
        lex = scan();
        if (lex != tknLpar) { 
            if (base == null && contains != null) { 
                f = ClassDescriptor.locateField(contains.containsFieldClass, ident);
                if (f != null) {
                    return new ElementNode(contains.containsExpr.getFieldName(), f);
                }
            } else if (cls != null) { 
                f = ClassDescriptor.locateField(cls, ident);                    
                if (f != null) { 
                    return new LoadNode(base, f);
                }
            }
        }
        Class[] profile = defaultProfile;
        Node[] arguments = noArguments;
        if (lex == tknLpar) { 
            ArrayList argumentList = new ArrayList();
            do { 
                argumentList.add(disjunction());
            } while (lex == tknComma);
            if (lex != tknRpar) {
                throw new CompileError("')' expected", pos);
            }
            lex = scan();
            profile = new Class[argumentList.size()];
            arguments = new Node[profile.length];
            boolean unknownProfile = false;
            for (int i = 0; i < profile.length; i++) { 
                Node arg = (Node)argumentList.get(i);
                arguments[i] = arg;
                Class argType;
                switch (arg.type) {
                  case Node.tpInt:
                    argType = long.class;
                    break;
                  case Node.tpReal:
                    argType = double.class;
                    break;
                  case Node.tpStr:
                    argType = String.class;
                    break;
                  case Node.tpDate:
                    argType = Date.class;
                    break;
                  case Node.tpBool:
                    argType = boolean.class;
                    break;
                  case Node.tpObj:
                    argType = arg.getType();
                    break;
                  case Node.tpArrayBool:
                    argType = boolean[].class;
                    break;
                  case Node.tpArrayChar:
                    argType = char[].class;
                    break;
                  case Node.tpArrayInt1:
                    argType = byte[].class;
                    break;
                  case Node.tpArrayInt2:
                    argType = short[].class;
                    break;
                  case Node.tpArrayInt4:
                    argType = int[].class;
                    break;
                  case Node.tpArrayInt8:
                    argType = long[].class;
                    break;
                  case Node.tpArrayReal4:
                    argType = float[].class;
                    break;
                  case Node.tpArrayReal8:
                    argType = double[].class;
                    break;
                  case Node.tpArrayStr:
                    argType = String[].class;
                    break;
                  case Node.tpArrayObj:
                    argType = Object[].class;
                    break;
                  case Node.tpUnknown:
                  case Node.tpAny:
                    argType = Object.class;
                    unknownProfile = true;
                    break;
                  default:
                    throw new CompileError("Invalid method argument type", pos);
                }
                profile[i] = argType;
            }
            if (unknownProfile) { 
                if (!cls.equals(Object.class) || base != null || contains == null) { 
                    return new InvokeAnyNode(base, ident, arguments, null);
                } else { 
                    return new InvokeAnyNode(base, ident, arguments, contains.containsExpr.getFieldName());     
                }
            }
        }
        Method m = null;
        if (base == null && contains != null) { 
            m = lookupMethod(contains.containsFieldClass, ident, profile);
            if (m != null) {
                return new InvokeElementNode(m, arguments, contains.containsExpr.getFieldName());
            }
            if (arguments == noArguments && cls != null) { 
                f = ClassDescriptor.locateField(cls, ident);                    
                if (f != null) { 
                    return new LoadNode(base, f);
                }
            }
        } 
        if (cls != null) { 
            m = lookupMethod(cls, ident, profile);
            if (m != null) { 
                return new InvokeNode(base, m, arguments);
            }
        }
        if (Object.class.equals(cls)) { 
            return profile.length == 0 
                ? (Node)new LoadAnyNode(base, ident, null) 
                : (Node)new InvokeAnyNode(base, ident, arguments, null);
        } else if (base == null && contains != null && contains.containsFieldClass.equals(Object.class)){ 
            String arrFieldName = contains.containsExpr.getFieldName();
            return profile.length == 0 
                ? (Node)new LoadAnyNode(base, ident, arrFieldName) 
                : (Node)new InvokeAnyNode(base, ident, arguments, arrFieldName);
        } else { 
            throw new CompileError("No field or method '"+ident+"' in class "+
                                   (cls == null ? contains.containsFieldClass : cls).getName(), pos);
        }         
    }


    final Node field(Node expr) {
        int p = pos;
        int type;
        int tag;
        Class cls = expr.getType();
        while (true) { 
            if (resolveMap != null && expr.type == Node.tpObj && cls != null) { 
                ResolveMapping rm = (ResolveMapping)resolveMap.get(cls);
                if (rm != null) { 
                    expr = new ResolveNode(expr, rm.resolver, rm.resolved);
                    cls = rm.resolved;
                }
            }
            switch (lex) {
              case tknDot:      
                if (scan() != tknIdent) { 
                    throw new CompileError("identifier expected", p);
                }
                if (expr.type != Node.tpObj && expr.type != Node.tpAny && expr.type != Node.tpCollection) { 
                    throw new CompileError("Left operand of '.' should be reference", p);
                }
                if (contains != null && contains.containsExpr.equals(expr)) { 
                    expr = component(null, null);
                } else { 
                    if (expr.type == Node.tpCollection) { 
                        throw new CompileError("Left operand of '.' should be reference", p);
                    }                        
                    expr = component(expr, cls);
                }
                cls = expr.getType();
                continue;
              case tknLbr:
                switch (expr.type) { 
                  case Node.tpArrayBool:
                    tag = Node.opGetAtBool;
                    type = Node.tpBool;
                    break;
                  case Node.tpArrayChar:
                    tag = Node.opGetAtChar;
                    type = Node.tpInt;
                    break;
                  case Node.tpStr:
                    tag = Node.opStrGetAt;
                    type = Node.tpInt;
                    break;
                  case Node.tpArrayInt1:
                    tag = Node.opGetAtInt1;
                    type = Node.tpInt;
                    break;
                  case Node.tpArrayInt2:
                    tag = Node.opGetAtInt2;
                    type = Node.tpInt;
                    break;
                  case Node.tpArrayInt4:
                    tag = Node.opGetAtInt4;
                    type = Node.tpInt;
                    break;
                  case Node.tpArrayInt8:
                    tag = Node.opGetAtInt8;
                    type = Node.tpInt;
                    break;
                  case Node.tpArrayReal4:
                    tag = Node.opGetAtReal4;
                    type = Node.tpReal;
                    break;
                  case Node.tpArrayReal8:
                    tag = Node.opGetAtReal8;
                    type = Node.tpReal;
                    break;
                  case Node.tpArrayStr:
                    tag = Node.opGetAtStr;
                    type = Node.tpStr;
                    break;
                  case Node.tpArrayObj:
                    tag = Node.opGetAtObj;
                    cls = cls.getComponentType();
                    type = cls.isArray() ? Node.tpArrayObj : cls.equals(Object.class) 
                        ? Node.tpAny : Node.tpObj;
                    break;
                  case Node.tpAny:
                    tag = Node.opGetAtObj;
                    type = Node.tpAny;
                    break;
                  default: 
                    throw new CompileError("Index can be applied only to arrays", 
                                           p);
                }
                p = pos;
                Node index = disjunction();
                if (lex != tknRbr) { 
                    throw new CompileError("']' expected", pos);
                }
                if (index.type == Node.tpAny) {
                    index = new ConvertAnyNode(Node.tpInt, index);
                } else if (index.type != Node.tpInt && index.type != Node.tpFreeVar) 
                {
                    throw new CompileError("Index should have integer type",p);
                }
                expr = new GetAtNode(type, tag, expr, index);
                lex = scan();
                continue;
              default:
                return expr;
            }   
        }
    }

    final Node containsElement() {
        int p = pos;
        Node containsExpr = term();
        Class arrClass = containsExpr.getType();
        if (arrClass == null || (!arrClass.isArray() && !arrClass.equals(Object.class) && !(Collection.class.isAssignableFrom(arrClass)))) 
        { 
            throw new CompileError("Contains clause can be applied only to arrays or collections", p);
        }
        Class arrElemType = arrClass.isArray() ? arrClass.getComponentType() : Object.class;
        p = pos;
        Node withCondition = null;

        ContainsNode outerContains = contains;
        ContainsNode innerContains = new ContainsNode(containsExpr, arrElemType);
        contains = innerContains;

        if (resolveMap != null) { 
            ResolveMapping rm = (ResolveMapping)resolveMap.get(arrElemType);
            if (rm != null) { 
                innerContains.resolver = rm.resolver;
                arrElemType = rm.resolved;
            }
        }
        
        if (lex == tknWith) { 
            innerContains.withExpr = checkType(Node.tpBool, disjunction());
        }
        if (lex == tknGroup) { 
            p = pos;
            if (scan() != tknBy) { 
                throw new CompileError("GROUP BY expected", p);
            }       
            p = pos;
            if (scan() != tknIdent) { 
                throw new CompileError("GROUP BY field expected", p);
            }                   
            if (arrElemType.equals(Object.class)) { 
                innerContains.groupByFieldName = ident;
            } else {
                Field groupByField = ClassDescriptor.locateField(arrElemType, ident);
                if (groupByField == null) { 
                    Method groupByMethod = lookupMethod(arrElemType, ident, defaultProfile);
                    if (groupByMethod == null) {
                        throw new CompileError("Field '"+ident+"' is not found", p);
                    } 
                    innerContains.groupByMethod = groupByMethod;                    
                    Class rt = groupByMethod.getReturnType();
                    if (rt.equals(void.class) 
                        || !(rt.isPrimitive() && !Comparable.class.isAssignableFrom(rt))) 
                    {
                        throw new CompileError("HashResult type " + rt + " of sort method should be comparable", p);
                    }
                } else { 
                    Class type = groupByField.getType();
                    if (!type.isPrimitive() && !Comparable.class.isAssignableFrom(type)) { 
                        throw new CompileError("Order by field type " + type + " should be comparable", p);
                    }
                    innerContains.groupByField = groupByField;
                    innerContains.groupByType = Node.getFieldType(type);
                }
            }
            if (scan() != tknHaving) { 
                throw new CompileError("HAVING expected", pos);
            }       
            innerContains.havingExpr = checkType(Node.tpBool, disjunction());       
        }
        contains = outerContains;
        return innerContains;
    }

    final Node aggregateFunction(int cop) {
        int p = pos;
        AggregateFunctionNode agr;
        if (contains == null 
            || (contains.groupByField == null && contains.groupByMethod == null && contains.groupByFieldName == null))
        {
            throw new CompileError("Aggregate function can be used only inside HAVING clause", p);
        }
        if (cop == tknCount) { 
            if (scan() != tknLpar || scan() != tknMul || scan() != tknRpar) {
                throw new CompileError("'count(*)' expected", p);
            }
            lex = scan();
            agr = new AggregateFunctionNode(Node.tpInt, Node.opCount, null);
        } else { 
            Node arg = term();
            if (arg.type == Node.tpAny) { 
                arg = new ConvertAnyNode(Node.tpReal, arg);
            } else if (arg.type != Node.tpInt && arg.type != Node.tpReal) { 
                throw new CompileError("Argument of aggregate function should have scalar type", p);
            }
            agr = new AggregateFunctionNode(arg.type, cop + Node.opAvg - tknAvg, arg);
        } 
        agr.index = contains.aggregateFunctions.size();
        contains.aggregateFunctions.add(agr);
        return agr;
    } 

    final Node checkType(int type, Node expr) {
        if (expr.type != type) { 
            if (expr.type == Node.tpAny) { 
                expr = new ConvertAnyNode(type, expr);
            } else if (expr.type == Node.tpUnknown) { 
                expr.type = type;
            } else { 
                throw new CompileError(Node.typeNames[type] + " expression expected", pos);
            }
        }
        return expr;
    }

    final Node term() {
        int cop = scan();
        int p = pos;
        Node expr;
        Binding bp;
        switch (cop) { 
          case tknEof:
          case tknOrder:
            lex = cop;
            return new EmptyNode();
          case tknParam:
            expr = new ParameterNode(parameters);
            break;
          case tknIdent:
            for (bp = bindings; bp != null; bp = bp.next) { 
                if (bp.name.equals(ident)) { 
                    lex = scan();
                    bp.used = true;
                    return new IndexNode(bp.loopId);
                }
            }
            expr = component(null, cls);
            return field(expr);
          case tknContains:
            return containsElement();
          case tknExists:
            if (scan() != tknIdent) { 
                throw new CompileError("Free variable name expected", p);
            }       
            bindings = bp = new Binding(ident, vars++, bindings);
            if (vars >= FilterIterator.maxIndexVars) { 
                throw new CompileError("Too many nested EXISTS clauses", p);
            }
            p = pos;
            if (scan() != tknCol) { 
                throw new CompileError("':' expected", p);
            }
            expr = checkType(Node.tpBool, term());
            if (bp.used) { 
                expr = new ExistsNode(expr, vars-1);
            }
            vars -= 1;      
            bindings = bp.next;
            return expr;
          case tknCurrent:
            lex = scan();
            return field(new CurrentNode(cls));
          case tknFalse:
            expr = new ConstantNode(Node.tpBool, Node.opFalse);
            break;
          case tknTrue:
            expr = new ConstantNode(Node.tpBool, Node.opTrue);
            break;
          case tknNull:
            expr = new ConstantNode(Node.tpObj, Node.opNull);
            break;
          case tknIconst:
            expr = new IntLiteralNode(ivalue);
            break;
          case tknFconst:
            expr = new RealLiteralNode(fvalue);
            break;
          case tknSconst:
            expr = new StrLiteralNode(svalue);
            lex = scan();
            return field(expr); 
          case tknSum:
          case tknMin:
          case tknMax:
          case tknAvg:
          case tknCount:
            return aggregateFunction(cop);
          case tknSin:
          case tknCos:
          case tknTan:
          case tknAsin:
          case tknAcos:
          case tknAtan:
          case tknExp:
          case tknLog:
          case tknSqrt:
          case tknCeil:
          case tknFloor:
            expr = term();
            if (expr.type == Node.tpInt) { 
                expr = int2real(expr);
            } else if (expr.type == Node.tpAny) { 
                expr = new ConvertAnyNode(Node.tpReal, expr);
            } else if (expr.type != Node.tpReal) { 
                throw new CompileError("Numeric argument expected", p);
            }
            return new UnaryOpNode(Node.tpReal, cop+Node.opRealSin-tknSin, 
                                   expr);
          case tknAbs:
            expr = term();
            if (expr.type == Node.tpInt) { 
                return new UnaryOpNode(Node.tpInt, Node.opIntAbs, expr);
            } else if (expr.type == Node.tpReal) { 
                return new UnaryOpNode(Node.tpReal, Node.opRealAbs, expr);
            } else if (expr.type == Node.tpAny) { 
                return new UnaryOpNode(Node.tpAny, Node.opAnyAbs, expr);
            } else { 
                throw new CompileError("ABS function can be applied only to integer or real expression", p);
            }
          case tknLength:
            expr = term();
            if (expr.type == Node.tpStr) { 
                return new UnaryOpNode(Node.tpInt, Node.opStrLength, expr);
            } else if (expr.type == Node.tpAny) { 
                return new UnaryOpNode(Node.tpInt, Node.opAnyLength, expr);
            } else if (expr.type >= Node.tpArrayBool) { 
                return new UnaryOpNode(Node.tpInt, Node.opLength, expr);
            } else { 
                throw new CompileError("LENGTH function is defined only for arrays and strings", p);
            } 
          case tknLower:
            return field(new UnaryOpNode(Node.tpStr, Node.opStrLower,
                                         checkType(Node.tpStr, term())));
          case tknUpper:
            return field(new UnaryOpNode(Node.tpStr, Node.opStrUpper,
                                         checkType(Node.tpStr, term())));
          case tknInteger:
            return new UnaryOpNode(Node.tpInt, Node.opRealToInt, checkType(Node.tpReal, term()));
          case tknReal:
            return new UnaryOpNode(Node.tpInt, Node.opIntToReal, checkType(Node.tpInt, term()));
          case tknString:
            expr = term();
            if (expr.type == Node.tpInt) { 
                return field(new UnaryOpNode(Node.tpStr, Node.opIntToStr, expr));
            } else if (expr.type == Node.tpReal) { 
                return field(new UnaryOpNode(Node.tpStr, Node.opRealToStr, expr));
            } else if (expr.type == Node.tpDate) { 
                return field(new UnaryOpNode(Node.tpStr, Node.opDateToStr, expr));
            } else if (expr.type == Node.tpAny) { 
                return field(new UnaryOpNode(Node.tpStr, Node.opAnyToStr, expr));
            }               
            throw new CompileError("STRING function can be applied only to integer or real expression", 
                                   p);
          case tknLpar:
          {
            expr = disjunction();
            Node list = null;
            while (lex == tknComma) { 
                list = new BinOpNode(Node.tpList, Node.opNop, list, expr);
                expr = disjunction();
            }
            if (lex != tknRpar) { 
                throw new CompileError("')' expected", pos);
            }
            if (list != null) { 
                expr = new BinOpNode(Node.tpList, Node.opNop, list, expr);
            }
            break;
          }
          case tknNot:
            expr = comparison();
            if (expr.type == Node.tpInt) { 
                if (expr.tag == Node.opIntConst) { 
                    IntLiteralNode ic = (IntLiteralNode)expr;
                    ic.value = ~ic.value;
                } else {
                    expr = new UnaryOpNode(Node.tpInt, Node.opIntNot, expr);
                } 
                return expr;
            } else if (expr.type == Node.tpBool) { 
                return new UnaryOpNode(Node.tpBool, Node.opBoolNot, expr);
            } else if (expr.type == Node.tpAny) { 
                return new UnaryOpNode(Node.tpAny, Node.opAnyNot, expr);
            } else { 
                throw new CompileError("NOT operator can be applied only to integer or boolean expressions", 
                                       p);
            }
          case tknAdd:
            throw new CompileError("Using of unary plus operator has no sense",
                                   p);
          case tknSub:
            expr = term();
            if (expr.type == Node.tpInt) { 
                if (expr.tag == Node.opIntConst) { 
                    IntLiteralNode ic = (IntLiteralNode)expr;
                    ic.value = -ic.value;
                } else {
                    expr = new UnaryOpNode(Node.tpInt, Node.opIntNeg, expr);
                } 
            } else if (expr.type == Node.tpReal) { 
                if (expr.tag == Node.opRealConst) { 
                    RealLiteralNode fc = (RealLiteralNode)expr;
                    fc.value = -fc.value;
                } else {
                    expr = new UnaryOpNode(Node.tpReal, Node.opRealNeg, expr);
                } 
            } else if (expr.type == Node.tpAny) { 
                expr = new UnaryOpNode(Node.tpAny, Node.opAnyNeg, expr);
            } else { 
                throw new CompileError("Unary minus can be applied only to numeric expressions", p);
            }
            return expr;
          default:
            throw new CompileError("operand expected", p);
        }
        lex = scan();
        return expr;
    }

    final IterableIterator<T> filter(IterableIterator iterator, Node condition) { 
        return new FilterIterator<T>(this, iterator, condition);
    }

    final IterableIterator<T> applyIndex(Node condition) 
    {
        Node filterCondition = null;
        Node expr = condition;
        Iterator result;
        if (expr.tag == Node.opBoolAnd) { 
            filterCondition = ((BinOpNode)expr).right;
            expr = ((BinOpNode)expr).left;
        }      
        if (expr.tag == Node.opContains) { 
            ContainsNode contains = (ContainsNode)expr;
            if (contains.withExpr == null) { 
                return null;
            }
            if (contains.havingExpr != null) { 
                filterCondition = condition;
            }
            expr = contains.withExpr;
        }
        if (expr instanceof BinOpNode) { 
            BinOpNode cmp = (BinOpNode)expr;
            String key = cmp.left.getFieldName();
            if (key != null && cmp.right instanceof LiteralNode) {
                GenericIndex index = getIndex(key);           
                if (index == null) { 
                    return null;
                }
                switch (expr.tag) { 
                  case Node.opAnyEq:
                  case Node.opIntEq:
                  case Node.opRealEq:
                  case Node.opStrEq:
                  case Node.opDateEq:
                  case Node.opBoolEq:
                  {
                      Key value = keyLiteral(index.getKeyType(), cmp.right, true);
                      if (value != null) { 
                          return filter(index.iterator(value, value, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                  case Node.opIntGt:
                  case Node.opRealGt:
                  case Node.opStrGt:
                  case Node.opDateGt:
                  case Node.opAnyGt:
                  {
                      Key value = keyLiteral(index.getKeyType(), cmp.right, false);
                      if (value != null) { 
                          return filter(index.iterator(value, null, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                  case Node.opIntGe:
                  case Node.opRealGe:
                  case Node.opStrGe:
                  case Node.opDateGe:
                  case Node.opAnyGe:
                  {
                      Key value = keyLiteral(index.getKeyType(), cmp.right, true);
                      if (value != null) { 
                          return filter(index.iterator(value, null, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                  case Node.opIntLt:
                  case Node.opRealLt:
                  case Node.opStrLt:
                  case Node.opDateLt:
                  case Node.opAnyLt:
                  {
                      Key value = keyLiteral(index.getKeyType(), cmp.right, false);
                      if (value != null) { 
                          return filter(index.iterator(null, value, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                  case Node.opIntLe:
                  case Node.opRealLe:
                  case Node.opStrLe:
                  case Node.opDateLe:
                  case Node.opAnyLe:
                  {
                      Key value = keyLiteral(index.getKeyType(), cmp.right, true);
                      if (value != null) { 
                          return filter(index.iterator(null, value, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                }
            }
        } else if (expr instanceof CompareNode) {             
            CompareNode cmp = (CompareNode)expr;
            String key = cmp.o1.getFieldName();
            if (key != null && cmp.o2 instanceof LiteralNode && (cmp.o3 == null || cmp.o3 instanceof LiteralNode))
            {
                GenericIndex index = getIndex(key);           
                if (index == null) { 
                    return null;
                }
                switch (expr.tag) { 
                  case Node.opIntBetween:
                  case Node.opStrBetween:
                  case Node.opRealBetween:
                  case Node.opDateBetween:
                  case Node.opAnyBetween:
                  {
                      Key value1 = keyLiteral(index.getKeyType(), cmp.o2, true);
                      Key value2 = keyLiteral(index.getKeyType(), cmp.o3, true);
                      if (value1 != null && value2 != null) { 
                          return filter(index.iterator(value1, value2, Index.ASCENT_ORDER), filterCondition);
                      }
                      return null;
                  }
                  case Node.opStrLike:
                  case Node.opStrLikeEsc:
                  {
                      String pattern = (String)((LiteralNode)cmp.o2).getValue();
                      char escape = cmp.o3 != null ? ((String)((LiteralNode)cmp.o3).getValue()).charAt(0) : '\\';
                      int pref = 0;
                      while (pref < pattern.length()) { 
                          char ch = pattern.charAt(pref);
                          if (ch == '%' || ch == '_') { 
                              break;
                          } else if (ch == escape) { 
                              pref += 2;
                          } else { 
                              pref += 1;
                          }
                      }
                      if (pref > 0) { 
                          if (pref == pattern.length()) { 
                              Key value = new Key(pattern);
                              return filter(index.iterator(value, value, Index.ASCENT_ORDER), filterCondition);
                          } else if (filterCondition == null) {
                              return filter(index.prefixIterator(pattern.substring(0, pref)), condition);
                          }
                      }
                  }
                }
            }
        }
        return null;
    }

    final void compile() {
        pos = 0;
        vars = 0;
        tree = checkType(Node.tpBool, disjunction());
        OrderNode last = null;
        order = null;
        if (lex == tknEof) {    
            return;
        }
        if (lex != tknOrder) { 
            throw new CompileError("ORDER BY expected", pos);
        }
        int tkn;
        int p = pos;
        if (scan() != tknBy) { 
            throw new CompileError("BY expected after ORDER", p);
        }
        do { 
            p = pos;
            if (scan() != tknIdent) { 
                throw new CompileError("field name expected", p);
            }
            OrderNode node;
            Field f = ClassDescriptor.locateField(cls, ident);
            if (f == null) {
                Method m = lookupMethod(cls, ident, defaultProfile);
                if (m == null) { 
                    if (!cls.equals(Object.class)) {
                        throw new CompileError("No field '"+ident+"' in class "+
                                               cls.getName(), p);
                    }
                    node = new OrderNode(ident);
                } else { 
                    node = new OrderNode(m);
                }
            } else {
                node = new OrderNode(ClassDescriptor.getTypeCode(f.getType()), f);
            }
            if (last != null) { 
                last.next = node;
            } else { 
                order = node;
            }
            last = node;
            p = pos;
            tkn = scan();
            if (tkn == tknDesc) { 
                node.ascent = false;
                tkn = scan();
            } else if (tkn == tknAsc) { 
                tkn = scan();
            }
        } while (tkn == tknComma);
        if (tkn != tknEof) { 
            throw new CompileError("',' expected", p);
        }
    }
}

package plugins.XMLSpider.org.garret.perst.impl;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;

import plugins.XMLSpider.org.garret.perst.CodeGenerator;
import plugins.XMLSpider.org.garret.perst.CodeGeneratorException;

class CodeGeneratorImpl implements CodeGenerator
{
    QueryImpl query;
    Class cls;

    CodeGeneratorImpl(QueryImpl query, Class cls) { 
        if (cls == null) { 
            throw new CodeGeneratorException("No class defined");
        }
        this.cls = cls;
        this.query = query;
    }
    
    public Code current() {
        return new CurrentNode(cls);
    }

    public Code literal(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return new IntLiteralNode(((Number)value).longValue());
        } else if (value instanceof Double || value instanceof Float) { 
            return new RealLiteralNode(((Number)value).doubleValue());
        } else if (value instanceof String) { 
            return new StrLiteralNode((String)value);
        } else if (value == null) { 
            return new ConstantNode(Node.tpObj, Node.opNull);
        } else if (value instanceof Boolean) { 
            return new ConstantNode(Node.tpBool, ((Boolean)value).booleanValue() ? Node.opTrue : Node.opFalse);
        } else if (value instanceof Date) {
            return new DateLiteralNode((Date)value);
        } else { 
            throw new CodeGeneratorException("Not suppored literal type: " + value);
        }
    }

    public Code list(Code... values) {
        Node list = null;
        for (int i = 0; i < values.length; i++) { 
            list = new BinOpNode(Node.tpList, Node.opNop, list, (Node)values[i]);
        }
        return list;
    }

    public Code parameter(int n, Class type) {
        int paramType;
        if (n < 1) { 
            throw new CodeGeneratorException("Parameter index should be positive number");            
        }
        if (type == int.class || type == long.class) {
            paramType = Node.tpInt;
        } else if (type == float.class || type == double.class) { 
            paramType = Node.tpReal;
        } else if (type == String.class) { 
            paramType = Node.tpStr;
        } else if (type == Date.class) { 
            paramType = Node.tpDate;
        } else if (Collection.class.isAssignableFrom(type)) { 
            paramType = Node.tpCollection;
        } else { 
            throw new CodeGeneratorException("Invalid argument types");            
        }
        return new ParameterNode(query.parameters, n-1, paramType);
    }


    public  Code field(String name) {
        return field(null, name);
    }

    public Code field(Code base, String name) {
        Class scope = (base == null) ? cls : ((Node)base).getType();
        Field f = ClassDescriptor.locateField(scope, name);                    
        if (f == null) {             
            throw new CodeGeneratorException("No such field " + name + " in class " + scope);
        }
        return new LoadNode((Node)base, f);
    }

    public Code invoke(Code base, String name, Code... arguments) {
        Class[] profile = new Class[arguments.length];
        Node[] args = new Node[arguments.length];
        for (int i = 0; i < profile.length; i++) { 
            Node arg = (Node)arguments[i];
            args[i] = arg;
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
            default:
                throw new CodeGeneratorException("Invalid method argument type");
            }
            profile[i] = argType;
        }
        Class scope = (base == null) ? cls : ((Node)base).getType();
        Method mth = QueryImpl.lookupMethod(scope, name, profile);
        if (mth == null) { 
            throw new CodeGeneratorException("Method " + name + " not found in class " + scope);
        }            
        return new InvokeNode((Node)base, mth, args);
    }

    public Code invoke(String name, Code... arguments) {
        return invoke(null, name, arguments);
    }


    public Code and(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntAnd, left, right);
        } else if (left.type == Node.tpBool && right.type == Node.tpBool) {
            return new BinOpNode(Node.tpBool, Node.opBoolAnd, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code or(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntOr, left, right);
        } else if (left.type == Node.tpBool && right.type == Node.tpBool) {
            return new BinOpNode(Node.tpBool, Node.opBoolOr, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }


    public Code add(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpReal, Node.opRealAdd, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntAdd, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpStr, Node.opStrConcat, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code sub(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpReal, Node.opRealSub, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntSub, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code mul(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpReal, Node.opRealMul, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntMul, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code div(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpReal, Node.opRealDiv, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntDiv, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code pow(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpReal, Node.opRealPow, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpInt, Node.opIntPow, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code like(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type != Node.tpStr || right.type != Node.tpStr) { 
            throw new CodeGeneratorException("Invalid argument types");
        }
        return new CompareNode(Node.opStrLike, left, right, null);
    }

    public Code like(Code opd1, Code opd2, Code opd3) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        Node esc = (Node)opd3;
        if (left.type != Node.tpStr || right.type != Node.tpStr || esc.tag != Node.opStrConst) { 
            throw new CodeGeneratorException("Invalid argument types");
        }
        return new CompareNode(Node.opStrLikeEsc, left, right, esc);
    }


    public Code eq(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealEq, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntEq, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrEq, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateEq, left, right);
        } else if (left.type == Node.tpObj && right.type == Node.tpObj) { 
            return new BinOpNode(Node.tpBool, Node.opObjEq, left, right);
        } else if (left.type == Node.tpBool && right.type == Node.tpBool) { 
            return new BinOpNode(Node.tpBool, Node.opBoolEq, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code ge(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealGe, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntGe, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrGe, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateGe, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code gt(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealGt, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntGt, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrGt, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateGt, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code lt(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealLt, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntLt, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrLt, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateLt, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code le(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealLe, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntLe, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrLe, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateLe, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code ne(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (left.type == Node.tpReal || right.type == Node.tpReal) { 
            if (left.type == Node.tpInt) { 
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpInt) { 
                right = QueryImpl.int2real(right);
            } else if (right.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opRealNe, left, right);
        } else if (left.type == Node.tpInt && right.type == Node.tpInt) { 
            return new BinOpNode(Node.tpBool, Node.opIntNe, left, right);
        } else if (left.type == Node.tpStr && right.type == Node.tpStr) {
            return new BinOpNode(Node.tpBool, Node.opStrNe, left, right);
        } else if (left.type == Node.tpDate || right.type == Node.tpDate) {
            if (left.type == Node.tpStr) {
                left = QueryImpl.str2date(left);
            } else if (left.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (right.type == Node.tpStr) {
                right = QueryImpl.str2date(right);
            } else if (right.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opDateNe, left, right);
        } else if (left.type == Node.tpObj && right.type == Node.tpObj) { 
            return new BinOpNode(Node.tpBool, Node.opObjNe, left, right);
        } else if (left.type == Node.tpBool && right.type == Node.tpBool) { 
            return new BinOpNode(Node.tpBool, Node.opBoolNe, left, right);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }


    public Code neg(Code opd) {
        Node expr = (Node)opd;
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
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }        
        return expr;
    }

    public Code abs(Code opd) {
        Node expr = (Node)opd;
        if (expr.type == Node.tpInt) { 
            return new UnaryOpNode(Node.tpInt, Node.opIntAbs, expr);
        } else if (expr.type == Node.tpReal) { 
            return new UnaryOpNode(Node.tpReal, Node.opRealAbs, expr);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }        
    }

    public Code not(Code opd) {
        Node expr = (Node)opd;
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
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }


    public Code between(Code opd1, Code opd2, Code opd3) {
        Node expr = (Node)opd1;
        Node low  = (Node)opd2;
        Node high = (Node)opd3;
        if (expr.type == Node.tpReal || low.type == Node.tpReal || high.type == Node.tpReal) {
            if (expr.type == Node.tpInt) { 
                expr = QueryImpl.int2real(expr);
            } else if (expr.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (low.type == Node.tpInt) {
                low = QueryImpl.int2real(low);
            } else if (low.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (high.type == Node.tpInt) {
                high = QueryImpl.int2real(high);
            } else if (high.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new CompareNode(Node.opRealBetween, expr, low, high);
        } else if (expr.type == Node.tpInt && low.type == Node.tpInt && high.type == Node.tpInt) {                   
            return new CompareNode(Node.opIntBetween, expr, low, high);
        } else if (expr.type == Node.tpStr && low.type == Node.tpStr && high.type == Node.tpStr) {
            return new CompareNode(Node.opStrBetween, expr, low, high);
        } else if (expr.type == Node.tpDate) { 
            if (low.type == Node.tpStr) {
                low = QueryImpl.str2date(low);
            } else if (low.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            if (high.type == Node.tpStr) {
                high = QueryImpl.str2date(high);
            } else if (high.type != Node.tpDate) {
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new CompareNode(Node.opDateBetween, expr, low, high);
        } else {         
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    private static Node listToTree(Node expr, BinOpNode list)
    {
        BinOpNode tree = null; 
        do { 
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
                    elem = QueryImpl.int2real(elem);
                }
            } else if (expr.type == Node.tpDate && elem.type == Node.tpDate) {
                cop = Node.opDateEq;
            } else if (expr.type == Node.tpStr && elem.type == Node.tpStr) {
                cop = Node.opStrEq;
            } else if (expr.type == Node.tpObj && elem.type == Node.tpObj) {
                cop = Node.opObjEq;
            } else if (expr.type == Node.tpBool && elem.type == Node.tpBool) {
                cop = Node.opBoolEq;
            }
            if (cop == Node.opNop) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            BinOpNode cmp = new BinOpNode(Node.tpBool, cop, expr, elem);
            if (tree == null) { 
                tree = cmp; 
            } else {
                tree = new BinOpNode(Node.tpBool, Node.opBoolOr, cmp, tree);
            }
        } while ((list = (BinOpNode)list.left) != null);
        return tree;
    }

    public Code in(Code opd1, Code opd2) {
        Node left = (Node)opd1;
        Node right = (Node)opd2;
        if (right == null) { 
            return new ConstantNode(Node.tpBool, Node.opFalse);
        }            
        switch (right.type) {
        case Node.tpCollection:
            return new BinOpNode(Node.tpBool, Node.opScanCollection, left, right);
        case Node.tpArrayBool:
            return new BinOpNode(Node.tpBool, Node.opScanArrayBool, checkType(Node.tpBool, left), right);
        case Node.tpArrayInt1:
            return new BinOpNode(Node.tpBool, Node.opScanArrayInt1, checkType(Node.tpInt, left), right);
        case Node.tpArrayChar:
            return new BinOpNode(Node.tpBool, Node.opScanArrayChar, checkType(Node.tpInt, left), right);
        case Node.tpArrayInt2:
            return new BinOpNode(Node.tpBool, Node.opScanArrayInt2, checkType(Node.tpInt, left), right);
        case Node.tpArrayInt4:
            return new BinOpNode(Node.tpBool, Node.opScanArrayInt4, checkType(Node.tpInt, left), right);
        case Node.tpArrayInt8:
            return new BinOpNode(Node.tpBool, Node.opScanArrayInt8, checkType(Node.tpInt, left), right);
        case Node.tpArrayReal4:
            if (left.type == Node.tpInt) {
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opScanArrayReal4, left, right);
        case Node.tpArrayReal8:
            if (left.type == Node.tpInt) {
                left = QueryImpl.int2real(left);
            } else if (left.type != Node.tpReal) { 
                throw new CodeGeneratorException("Invalid argument types");
            }
            return new BinOpNode(Node.tpBool, Node.opScanArrayReal8, left, right);
        case Node.tpArrayObj:
            return new BinOpNode(Node.tpBool, Node.opScanArrayObj, checkType(Node.tpObj, left), right);
        case Node.tpArrayStr:
            return new BinOpNode(Node.tpBool, Node.opScanArrayStr, checkType(Node.tpStr, left), right);
        case Node.tpStr:
            return new BinOpNode(Node.tpBool, Node.opInString, checkType(Node.tpStr, left), right);
        case Node.tpList:
            return listToTree(left, (BinOpNode)right);
        default:
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    Node mathFunc(int cop, Code opd) { 
        Node expr = (Node)opd;
        if (expr.type == Node.tpInt) { 
            expr = QueryImpl.int2real(expr);
        } else if (expr.type != Node.tpReal) { 
            throw new CodeGeneratorException("Invalid argument types");
        }
        return new UnaryOpNode(Node.tpReal, cop, expr);
    }

    public Code sin(Code opd) {
        return mathFunc(Node.opRealSin, opd);
    }

    public Code cos(Code opd) {
        return mathFunc(Node.opRealCos, opd);
    }

    public Code tan(Code opd) {
        return mathFunc(Node.opRealTan, opd);
    }

    public Code asin(Code opd) {
        return mathFunc(Node.opRealAsin, opd);
    }

    public Code acos(Code opd) {
        return mathFunc(Node.opRealAcos, opd);
    }

    public Code atan(Code opd) {
        return mathFunc(Node.opRealAtan, opd);
    }

    public Code sqrt(Code opd) {
        return mathFunc(Node.opRealSqrt, opd);
    }

    public Code exp(Code opd) {
        return mathFunc(Node.opRealExp, opd);
    }

    public Code log(Code opd) {
        return mathFunc(Node.opRealLog, opd);
    }

    public Code ceil(Code opd) {
        return mathFunc(Node.opRealCeil, opd);
    }

    public Code floor(Code opd) {
        return mathFunc(Node.opRealFloor, opd);
    }

    private static Node checkType(int type, Code opd) {
        Node expr = (Node)opd;
        if (expr.type != type) { 
            throw new CodeGeneratorException("Invalid argument types");
        }
        return expr;
    }

    public Code upper(Code opd) {
        return new UnaryOpNode(Node.tpStr, Node.opStrUpper, checkType(Node.tpStr, opd));
    }

    public Code lower(Code opd) {
        return new UnaryOpNode(Node.tpStr, Node.opStrLower, checkType(Node.tpStr, opd));
    }

    public Code length(Code opd) {
        Node expr = (Node)opd;
        if (expr.type == Node.tpStr) { 
            return new UnaryOpNode(Node.tpInt, Node.opStrLength, expr);
        } else if (expr.type >= Node.tpArrayBool && expr.type <= Node.tpArrayObj) { 
            return new UnaryOpNode(Node.tpInt, Node.opLength, expr);
        } else { 
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code string(Code opd) {
        Node expr = (Node)opd;
        if (expr.type == Node.tpInt) { 
            return new UnaryOpNode(Node.tpStr, Node.opIntToStr, expr);
        } else if (expr.type == Node.tpReal) { 
            return new UnaryOpNode(Node.tpStr, Node.opRealToStr, expr);
        } else if (expr.type == Node.tpDate) { 
            return new UnaryOpNode(Node.tpStr, Node.opDateToStr, expr);
        } else {
            throw new CodeGeneratorException("Invalid argument types");
        }
    }

    public Code getAt(Code opd1, Code opd2) {
        Node expr = (Node)opd1;
        Node index = checkType(Node.tpInt, opd2);
        int tag;
        int type;
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
        default: 
            throw new CodeGeneratorException("Invalid argument types");
        }
        return new GetAtNode(type, tag, expr, index);                
    }

    public Code integer(Code opd) {
        return new UnaryOpNode(Node.tpStr, Node.opStrLower, checkType(Node.tpReal, opd));
    }

    public Code real(Code opd) {
        return new UnaryOpNode(Node.tpStr, Node.opStrLower, checkType(Node.tpReal, opd));
    }

    public void predicate(Code code) { 
        query.tree = checkType(Node.tpBool, code);
    }

    public void orderBy(String name, boolean ascent) { 
        Field f = ClassDescriptor.locateField(cls, name);
        OrderNode node;
        if (f == null) {
            Method m = QueryImpl.lookupMethod(cls, name, QueryImpl.defaultProfile);
            if (m == null) { 
                throw new CodeGeneratorException("No such field " + name + " in class " + cls);
            } else { 
                node = new OrderNode(m);
            }
        } else {
            node = new OrderNode(ClassDescriptor.getTypeCode(f.getType()), f);
        }
        node.ascent = ascent;
        if (query.order == null) { 
            query.order = node;
        } else { 
            OrderNode last;
            for (last = query.order; last.next != null; last = last.next);
            last.next = node;
        }
    }
    
    public void orderBy(String name) { 
        orderBy(name, true);
    }     
}

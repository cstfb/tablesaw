package tech.tablesaw.pandas.dataframe;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.columns.numbers.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * a pandas-style api based on {@link tech.tablesaw.api.Table}
 *
 * about inPlace / copy option: //todo
 *
 * <p>
 *     problems :
 *          1. DataFrame init : type detector(from Class<T>) and data convert layer need to be optimized,
 *                      
 *          2. Some type like BigDecimal, generic Object can not be handled correctly,
 *                  for example , there is no ObjectColumn or BigDecimalColumn ,
 *                  in pandas , column can hold complex object like 'list'
 *
 *          3. primitive type can not have null value , instead of missing_value , may be cause unexpected behavior
 * </p>
 */
//@Immutable
public class DataFrame extends Table {

    /**
     * initiate an empty table
     */
    public DataFrame() {
        super("table");
    }

    /**
     * Returns a new Table initialized with the given names and columns
     *
     * @param name    The name of the table
     * @param columns One or more columns, all of which must have either the same length or size 0
     */
    public DataFrame(String name, Column<?>... columns) {
        super(name, columns);
    }

    /**
     * Returns a new, empty table (without rows or columns) with the given name
     */
    public static Table create(String tableName) {
        return new DataFrame(tableName);
    }

    /**
     * Returns a new table with the given columns and given name
     *
     * @param columns One or more columns, all of the same @code{column.size()}
     */
    public static Table create(final String tableName, final Column<?>... columns) {
        return new DataFrame(tableName, columns);
    }

    /**
     * Returns a new Table with the given list; column name comes from property name;
     * cell value comes from property value;
     *
     * type decided by first element in data;
     *
     * @param data
     */
    public DataFrame(List<?> data) {
        super("table");
        if (data == null || data.isEmpty()) {
            return;
        }
        // parse element
        Object firstEle = data.get(0);
        Map<String, Field> fieldMap = getAllFieldOfBean(firstEle);

        try {
            // construct columns according to type of fields
            for (Object obj : data) {
                for (String columnName : fieldMap.keySet()) {
                    Column column = column(columnName);
                    Field field = fieldMap.get(columnName);
                    Object columnValue = field.get(obj);
                    Class fieldType = field.getType();

                    // data pre-processed
                    if (Date.class.isAssignableFrom(fieldType)) {
                        // date value -> convert LocalDateTime
                        if (columnValue != null) {
                            columnValue = LocalDateTime.ofInstant(((Date) columnValue).toInstant(), ZoneId.systemDefault());
                        }
                    }

                    //primitive types can not have null
                    if (null == columnValue) {
                        if (Double.class.isAssignableFrom(fieldType)) {
                            columnValue = DoubleColumnType.missingValueIndicator();
                        } else if (Float.class.isAssignableFrom(fieldType)) {
                            columnValue = FloatColumnType.missingValueIndicator();
                        } else if (Integer.class.isAssignableFrom(fieldType)) {
                            columnValue = IntColumnType.missingValueIndicator();
                        } else if (Long.class.isAssignableFrom(fieldType)) {
                            columnValue = LongColumnType.missingValueIndicator();
                        } else if (Short.class.isAssignableFrom(fieldType)) {
                            columnValue = ShortColumnType.missingValueIndicator();
                        }
                    }

                    // route to columns
                    if (null == column) {
                        // create new column according to data type + auto detect
                        if (boolean.class.isAssignableFrom(fieldType)
                                || Boolean.class.isAssignableFrom(fieldType)) {
                            column = BooleanColumn.create(columnName);
                        } else if (Date.class.isAssignableFrom(fieldType)) {
                            column = DateTimeColumn.create(columnName);
                        } else if (double.class.isAssignableFrom(fieldType)
                                || Double.class.isAssignableFrom(fieldType)) {
                            column = DoubleColumn.create(columnName);
                        } else if (float.class.isAssignableFrom(fieldType)
                                || Float.class.isAssignableFrom(fieldType)) {
                            column = FloatColumn.create(columnName);
                        } else if (int.class.isAssignableFrom(fieldType)
                                || Integer.class.isAssignableFrom(fieldType)) {
                            column = IntColumn.create(columnName);
                        } else if (long.class.isAssignableFrom(fieldType)
                                || Long.class.isAssignableFrom(fieldType)) {
                            column = LongColumn.create(columnName);
                        } else if (short.class.isAssignableFrom(fieldType)
                                || Short.class.isAssignableFrom(fieldType)) {
                            column = ShortColumn.create(columnName);
                        } else if (String.class.isAssignableFrom(fieldType)) {
                            column = StringColumn.create(columnName);
                        } else {
                            // todo objectColumn
                            continue;
                        }

                        // add column
                        column.append(columnValue);
                        addColumns(column);
                    } else {
                        column.append(columnValue);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("data parsed failed", e);
        }
    }

    /**
     * Returns the column with the given columnName, ignoring case
     *
     * return null if absent
     */
    @Override
    public Column<?> column(String columnName) {
        for (Column<?> column : columns()) {
            String name = column.name().trim();
            if (name.equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    /**
     * get all properties and its type of a bean, inlcude its parent class, Object excluded
     */
    Map<String, Field> getAllFieldOfBean(Object obj) {
        Map<String, Field> fieldMap = new HashMap<>(8);
        Class tmpClz = obj.getClass();
        String finalParent = "java.lang.object";
        // get field until loop to Root
        while (tmpClz != null && !tmpClz.getName().toLowerCase().equals(finalParent)) {
            for (Field field : tmpClz.getDeclaredFields()) {
                field.setAccessible(true);
                // get public/private/protect field
                int modifiers = field.getModifiers();
                if (modifiers == Modifier.PUBLIC || modifiers == Modifier.PRIVATE || modifiers == Modifier.PROTECTED) {
                    fieldMap.putIfAbsent(field.getName(), field);
                }
            }
            tmpClz = tmpClz.getSuperclass();
        }

        return fieldMap;
    }

    // columns / columnNames exists

    public List<Class> dTypes() {
        return null;
    }

    /**
     * return an int representing the number of axes dimensions.
     *
     * @return Return 1 if column. Otherwise return 2 DataFrame.
     */
    public int nDim() {
        return 2;
    }

    /**
     * Return an int representing the number of elements in this object.
     *
     * @return rows times number of columns , missing values excluded
     */
    public int size() {
        int ret = 0;
        for (Column column : columns()) {
            ret += column.size() - column.countMissing();
        }
        return ret;
    }

//    /**
//     * Return a tuple representing the dimensionality of the DataFrame.
//     *
//     * @return
//     */
//    public String shape() {
//        if (isEmpty()) {
//            return "(0,0)";
//        }
//        return "(" + rowCount() + "," + columnCount() + ")";
//    }


    /*************************Conversion**************************/
    /**
     * cast a DataFrame to a specified type
     *
     * @param dtypes
     * @param copy
     * @return
     */
    public DataFrame asType(Map<Column, ColumnType> dtypes, boolean copy) {
        return null;
    }

    public DataFrame isna() {
        return null;
    }

    public DataFrame notna() {
        return null;
    }

    /**********************/



    public static void main(String[] args) {
        DataFrame dataFrame = new DataFrame("");
//        dataFrame.
    }
}

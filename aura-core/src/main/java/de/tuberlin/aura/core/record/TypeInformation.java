package de.tuberlin.aura.core.record;

import com.esotericsoftware.reflectasm.FieldAccess;
import de.tuberlin.aura.core.record.tuples.Tuple2;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.record.tuples.Tuple4;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 *
 */
public final class TypeInformation implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // --------------------------------------------------

    public final Class<?> type;

    public final List<TypeInformation> fieldTypes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TypeInformation(final Class<?> type) {
        this(type, null);
    }

    public TypeInformation(final Class<?> type, TypeInformation... fieldTypes) {
        this(type, fieldTypes.length > 1 ? Arrays.asList(fieldTypes) : null);
    }

    public TypeInformation(final Class<?> type, List<TypeInformation> fieldTypes) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;

        this.fieldTypes = fieldTypes != null ? Collections.unmodifiableList(fieldTypes) : null;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Object selectField(final int[] selectorChain, final Object target) {
        // sanity check.
        if (selectorChain == null)
            throw new IllegalArgumentException("selectorChain == null");
        if (target == null)
            throw new IllegalArgumentException("target == null");

        TypeInformation ti = this;
        Object obj = target;
        for (final int fieldIndex : selectorChain) {
            FieldAccess fa = RowRecordModel.RecordTypeBuilder.getFieldAccessor(ti.type);
            obj = fa.get(obj, fieldIndex);
            ti = ti.fieldTypes.get(fieldIndex);
        }

        return obj;
    }

    public List<Class<?>> extractTypes() {
        return _extractTypes(this);

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(type.getSimpleName());
        sb.append("\n");
        for (final TypeInformation ti : fieldTypes) {
            sb.append(ti.toString());
        }
        return sb.toString();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<Class<?>> _extractTypes(final TypeInformation typeInfo) {
        // sanity check.
        if (typeInfo == null)
            throw new IllegalArgumentException("typeInfo == null");

        final List<Class<?>> typeList = new ArrayList<>();
        typeList.add(typeInfo.type);
        if (typeInfo.fieldTypes != null) {
            for (final TypeInformation ti : typeInfo.fieldTypes) {
                typeList.addAll(_extractTypes(ti));
            }
        }
        return typeList;
    }

    // ---------------------------------------------------
    // Public Static Methods.
    // ---------------------------------------------------

    public static TypeInformation buildFromInstance(final Object obj) {
        // sanity check.
        if (obj == null)
            throw new IllegalArgumentException("obj == null");

        final List<TypeInformation> fields = new ArrayList<>();
        try {
            for (final Field f : obj.getClass().getFields()) {
                if (Modifier.isPublic(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())) {
                    final Object fieldObj = f.get(obj);
                    final TypeInformation fieldTypeInformation = buildFromInstance(fieldObj);
                    fields.add(fieldTypeInformation);
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        return new TypeInformation(obj.getClass(), fields);
    }

    // ---------------------------------------------------
    // Test Main.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        /*final Tuple2<Integer, Tuple3<Integer, Integer, Tuple4<Integer, Integer, Integer, Integer>>> tuple =
                new Tuple2<>(1, new Tuple3<>(1, 1, new Tuple4<>(1, 1, 1, 15)));

        final TypeInformation ti =
                new TypeInformation(Tuple2.class,
                                    new TypeInformation(Integer.class),
                                    new TypeInformation(Tuple3.class,
                                                        new TypeInformation(Integer.class),
                                                        new TypeInformation(Integer.class),
                                                        new TypeInformation(Tuple4.class,
                                                                            new TypeInformation(Integer.class),
                                                                            new TypeInformation(Integer.class),
                                                                            new TypeInformation(Integer.class),
                                                                            new TypeInformation(Integer.class))));

        final List<Class<?>> classList = ti.extractTypes();

        for (final Class<?> clazz : classList) {
            System.out.println(clazz.getSimpleName());
        }*/

        //final Object result = ti.selectField(new int[] {1, 2}, tuple);
        //System.out.println("--> " + result);

        final Random rand1 = new Random(12312);
        final Random rand2 = new Random(13454);
        for(int i = 0; i < 1000; ++i) {
            if(rand2.nextInt(10000) == 9935) {
                System.out.println("#");
            }
        }
    }
}

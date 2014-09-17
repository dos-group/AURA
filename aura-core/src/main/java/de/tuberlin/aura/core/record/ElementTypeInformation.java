package de.tuberlin.aura.core.record;

import com.esotericsoftware.reflectasm.FieldAccess;
import de.tuberlin.aura.core.common.utils.ArrayUtils;
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
public final class ElementTypeInformation implements TypeInformation {

    // ---------------------------------------------------
    // Fields.
    // --------------------------------------------------

    private final Class<?> type;

    public final List<ElementTypeInformation> fieldTypes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ElementTypeInformation(final Class<?> type) {
        this(type, (ElementTypeInformation)null);
    }

    public ElementTypeInformation(final Class<?> type, ElementTypeInformation... fieldTypes) {
        this(type, fieldTypes.length > 1 ? Arrays.asList(fieldTypes) : null);
    }

    public ElementTypeInformation(final Class<?> type, List<ElementTypeInformation> fieldTypes) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;

        this.fieldTypes = fieldTypes != null ? Collections.unmodifiableList(fieldTypes) : null;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Object selectField(final int[] selectorChain, final Object target) {
        // sanity check.
        if (selectorChain == null)
            throw new IllegalArgumentException("selectorChain == null");
        if (target == null)
            throw new IllegalArgumentException("target == null");

        ElementTypeInformation ti = this;
        Object obj = target;
        for (final int fieldIndex : selectorChain) {
            FieldAccess fa = RowRecordModel.RecordTypeBuilder.getFieldAccessor(ti.type);
            obj = fa.get(obj, fieldIndex);
            ti = ti.fieldTypes.get(fieldIndex);
        }

        return obj;
    }

    @Override
    public int[] buildFieldSelectorChain(final String accessPath) {
        // sanity check.
        if (accessPath == null)
            throw new IllegalArgumentException("accessPath == null");

        final StringTokenizer st = new StringTokenizer(accessPath, ".", false);
        final List<Integer> selectorChain = new ArrayList<>();
        ElementTypeInformation ti = this;

        while (st.hasMoreElements()) {
            FieldAccess fa = RowRecordModel.RecordTypeBuilder.getFieldAccessor(ti.type);
            final int fieldIndex = fa.getIndex(st.nextToken());
            selectorChain.add(fieldIndex);
            ti = ti.fieldTypes.get(fieldIndex);
        }

        return ArrayUtils.toIntArray(selectorChain);
    }

    @Override
    public Class<?> getType() {
        return type;
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

    private List<Class<?>> _extractTypes(final ElementTypeInformation typeInfo) {
        // sanity check.
        if (typeInfo == null)
            throw new IllegalArgumentException("typeInfo == null");

        final List<Class<?>> typeList = new ArrayList<>();
        typeList.add(typeInfo.type);
        if (typeInfo.fieldTypes != null) {
            for (final ElementTypeInformation ti : typeInfo.fieldTypes) {
                typeList.addAll(_extractTypes(ti));
            }
        }
        return typeList;
    }

    // ---------------------------------------------------
    // Public Static Methods.
    // ---------------------------------------------------

    public static ElementTypeInformation buildFromInstance(final Object obj) {
        // sanity check.
        if (obj == null)
            throw new IllegalArgumentException("obj == null");

        final List<ElementTypeInformation> fields = new ArrayList<>();
        try {
            for (final Field f : obj.getClass().getFields()) {
                if (Modifier.isPublic(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())) {
                    final Object fieldObj = f.get(obj);
                    final ElementTypeInformation fieldTypeInformation = buildFromInstance(fieldObj);
                    fields.add(fieldTypeInformation);
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        return new ElementTypeInformation(obj.getClass(), fields);
    }

    // ---------------------------------------------------
    // Test Main.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        /*final Tuple2<Integer, Tuple3<Integer, Integer, Tuple4<Integer, Integer, Integer, Integer>>> tuple =
                new Tuple2<>(1, new Tuple3<>(1, 1, new Tuple4<>(1, 1, 1, 15)));

        final ITypeInformation ti =
                new ITypeInformation(Tuple2.class,
                                    new ITypeInformation(Integer.class),
                                    new ITypeInformation(Tuple3.class,
                                                        new ITypeInformation(Integer.class),
                                                        new ITypeInformation(Integer.class),
                                                        new ITypeInformation(Tuple4.class,
                                                                            new ITypeInformation(Integer.class),
                                                                            new ITypeInformation(Integer.class),
                                                                            new ITypeInformation(Integer.class),
                                                                            new ITypeInformation(Integer.class))));

        final List<Class<?>> classList = ti.extractTypes();
        for (final Class<?> clazz : classList) {
            System.out.println(clazz.getSimpleName());
        }
        final Object result = ti.selectField(new int[] {1, 2}, tuple);
        System.out.println("--> " + result);*/
    }
}

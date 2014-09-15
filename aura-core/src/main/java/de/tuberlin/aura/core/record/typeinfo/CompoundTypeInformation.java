package de.tuberlin.aura.core.record.typeinfo;

import com.esotericsoftware.reflectasm.FieldAccess;
import de.tuberlin.aura.core.common.utils.ArrayUtils;
import de.tuberlin.aura.core.record.RowRecordModel;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 *
 */
public class CompoundTypeInformation implements ITypeInformation {

    // ---------------------------------------------------
    // Fields.
    // --------------------------------------------------

    public final Class<?> type;

    public final List<ITypeInformation> fieldTypes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public CompoundTypeInformation(final Class<?> type) {
        this(type, (ITypeInformation)null);
    }

    public CompoundTypeInformation(final Class<?> type, ITypeInformation... fieldTypes) {
        this(type, fieldTypes.length > 1 ? Arrays.asList(fieldTypes) : null);
    }

    public CompoundTypeInformation(final Class<?> type, final List<ITypeInformation> fieldTypes) {
        // sanity check.
        if (type == null)
            throw new IllegalArgumentException("type == null");

        this.type = type;

        this.fieldTypes = fieldTypes != null ? Collections.unmodifiableList(fieldTypes) : null;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Class<?> getType() {
        return type;
    }

    public Object selectField(final int[] selectorChain, final Object target) {
        // sanity check.
        if (selectorChain == null)
            throw new IllegalArgumentException("selectorChain == null");
        if (target == null)
            throw new IllegalArgumentException("target == null");

        CompoundTypeInformation ti = this;
        Object obj = target;
        for (final int fieldIndex : selectorChain) {
            FieldAccess fa = RowRecordModel.RecordTypeBuilder.getFieldAccessor(ti.type);
            obj = fa.get(obj, fieldIndex);
            ti = (CompoundTypeInformation)ti.fieldTypes.get(fieldIndex);
        }

        return obj;
    }

    public int[] buildSelectorChain(final String accessPath) {
        // sanity check.
        if (accessPath == null)
            throw new IllegalArgumentException("accessPath == null");

        final StringTokenizer st = new StringTokenizer(accessPath, ".", false);
        final List<Integer> selectorChain = new ArrayList<>();
        CompoundTypeInformation ti = this;

        while (st.hasMoreElements()) {
            FieldAccess fa = RowRecordModel.RecordTypeBuilder.getFieldAccessor(ti.type);
            final int fieldIndex = fa.getIndex(st.nextToken());
            selectorChain.add(fieldIndex);
            ti = (CompoundTypeInformation)ti.fieldTypes.get(fieldIndex);
        }

        return ArrayUtils.toIntArray(selectorChain);
    }

    public List<Class<?>> extractTypes() {
        return _extractTypes(this);

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(type.getSimpleName());
        sb.append("\n");
        for (final ITypeInformation ti : fieldTypes) {
            sb.append(ti.toString());
        }
        return sb.toString();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<Class<?>> _extractTypes(final ITypeInformation typeInfo) {
        // sanity check.
        if (typeInfo == null)
            throw new IllegalArgumentException("typeInfo == null");

        final List<Class<?>> typeList = new ArrayList<>();
        typeList.add(typeInfo.getType());
        if (typeInfo instanceof CompoundTypeInformation) {
            final CompoundTypeInformation cti = (CompoundTypeInformation)typeInfo;
            if (cti.fieldTypes != null)
            for (final ITypeInformation ti : cti.fieldTypes) {
                typeList.addAll(_extractTypes(ti));
            }
        }
        return typeList;
    }

    // ---------------------------------------------------
    // Public Static Methods.
    // ---------------------------------------------------

    public static ITypeInformation buildFromInstance(final Object obj) {
        // sanity check.
        if (obj == null)
            throw new IllegalArgumentException("obj == null");

        final List<ITypeInformation> fields = new ArrayList<>();
        try {
            for (final Field f : obj.getClass().getFields()) {
                if (Modifier.isPublic(f.getModifiers()) && !Modifier.isStatic(f.getModifiers())) {
                    final Object fieldObj = f.get(obj);
                    final ITypeInformation fieldTypeInformation = buildFromInstance(fieldObj);
                    fields.add(fieldTypeInformation);
                }
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }

        return new CompoundTypeInformation(obj.getClass(), fields);
    }
}

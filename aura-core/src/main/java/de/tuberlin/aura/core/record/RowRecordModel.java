package de.tuberlin.aura.core.record;

import static org.objectweb.asm.Opcodes.*;

import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import com.esotericsoftware.reflectasm.FieldAccess;

public final class RowRecordModel {

    public static interface IKeySelector {

        public abstract int[] key();
    }

    public static final class RecordTypeBuilder {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------
        
        private static final Map<Class<?>, FieldAccess> fieldAccessorRegistry =
                new HashMap<Class<?>, FieldAccess>();

        private static final Map<Class<?>, ConstructorAccess<?>> constructorAccessRegistry
                = new HashMap<Class<?>, ConstructorAccess<?>>();

        private static final Map<Class<?>, byte[]> byteCodeRepository
                = new HashMap<>();

        private static final ClassLoader clazzLoader = ClassLoader.getSystemClassLoader();

        private static int typeCounter = 0;

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public static FieldAccess getFieldAccessor(final Class<?> clazz) {
            // sanity check.
            if (clazz == null)
                throw new IllegalArgumentException("clazz == null");

            final FieldAccess fa = fieldAccessorRegistry.get(clazz);

            if (fa == null) {
                final FieldAccess accessor = FieldAccess.get(clazz);
                fieldAccessorRegistry.put(clazz, accessor);
                return accessor;
                //throw new IllegalStateException("no FieldAccessor found ");
            }

            return fa;
        }

        public static byte[] getRecordByteCode(final Class<?> clazz) {
            // sanity check.
            if (clazz == null)
                throw new IllegalArgumentException("clazz == null");

            return byteCodeRepository.get(clazz);
        }

        public static synchronized Class<?> buildRecordType(final Class<?>[] fieldTypes) {
            // sanity check.
            if (fieldTypes == null)
                throw new IllegalArgumentException("fieldTypes == null");

            final String recordTypeName = "record" + typeCounter++;
            final ClassWriter cw = new ClassWriter(0);

            emitClassDef(cw, recordTypeName);
            emitConstructor(cw);

            int fieldIndex = 0;
            for (final Class<?> fieldType : fieldTypes) {
                emitPublicField(cw, fieldType, "field" + fieldIndex++);
            }

            cw.visitEnd();
            final byte[] byteCode = cw.toByteArray();

            final Class<?> clazz = new ClassLoader(clazzLoader) {
                public Class<?> defineClass() {
                    final Class<?> recordType = defineClass(recordTypeName, byteCode, 0, byteCode.length);
                    return recordType;
                }
            }.defineClass();

            FieldAccess fieldAccessor = fieldAccessorRegistry.get(clazz);
            if (fieldAccessor == null) {
                fieldAccessor = FieldAccess.get(clazz);
                fieldAccessorRegistry.put(clazz, fieldAccessor);
            }

            ConstructorAccess<?> constructorAccessor = constructorAccessRegistry.get(clazz);
            if (constructorAccessor == null) {
                constructorAccessor = ConstructorAccess.get(clazz);
                constructorAccessRegistry.put(clazz, constructorAccessor);
            }

            byteCodeRepository.put(clazz, byteCode);

            return clazz;
        }

        public static synchronized void addRecordType(final Class<?> clazz, final byte[] byteCode) {
            // sanity check.
            if (clazz == null)
                throw new IllegalArgumentException("clazz == null");
            if (byteCode == null)
                throw new IllegalArgumentException("byteCode == null");

            FieldAccess fieldAccessor = fieldAccessorRegistry.get(clazz);
            if (fieldAccessor == null) {
                fieldAccessor = FieldAccess.get(clazz);
                fieldAccessorRegistry.put(clazz, fieldAccessor);
            }

            ConstructorAccess<?> constructorAccessor = constructorAccessRegistry.get(clazz);
            if (constructorAccessor == null) {
                constructorAccessor = ConstructorAccess.get(clazz);
                constructorAccessRegistry.put(clazz, constructorAccessor);
            }

            byteCodeRepository.put(clazz, byteCode);
        }

        public static Record createRecord(final Class<?> clazz) {
            // sanity check.
            if (clazz == null)
                throw new IllegalArgumentException("clazz == null");

            final ConstructorAccess<?> ctorAccess = constructorAccessRegistry.get(clazz);
            return new Record(ctorAccess.newInstance());
        }

        // ---------------------------------------------------
        // Private Methods.
        // ---------------------------------------------------

        private static void emitClassDef(final ClassWriter cw, final String className) {
            cw.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER,
                    className, null, "java/lang/Object", null);
        }

        private static void emitConstructor(final ClassWriter cw) {
            final MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", "()V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }

        private static void emitPublicField(final ClassWriter cw, final Class<?> fieldType, final String fieldName) {
            cw.visitField(Opcodes.ACC_PUBLIC, fieldName, Type.getDescriptor(fieldType), null, null);
        }
    }

    public static final class Record {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------
        
        private final Object instance;

        private final FieldAccess fieldAccessor;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public Record(final Object instance) {
            // sanity check.
            if (instance == null)
                throw new IllegalArgumentException("instance == null");

            this.instance = instance;

            this.fieldAccessor = RecordTypeBuilder.getFieldAccessor(instance.getClass());
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------
        
        public void set(int fieldIndex, Object value) {
            this.fieldAccessor.set(instance, fieldIndex, value);
        }

        public void setString(int fieldIndex, String value) {
            set(fieldIndex, value);
        }

        public void setBoolean(int fieldIndex, boolean value) {
            this.fieldAccessor.setBoolean(instance, fieldIndex, value);
        }

        public void setByte(int fieldIndex, byte value) {
            this.fieldAccessor.setByte(instance, fieldIndex, value);
        }

        public void setShort(int fieldIndex, short value) {
            this.fieldAccessor.setShort(instance, fieldIndex, value);
        }

        public void setInt(int fieldIndex, int value) {
            this.fieldAccessor.setInt(instance, fieldIndex, value);
        }

        public void setLong(int fieldIndex, long value) {
            this.fieldAccessor.setLong(instance, fieldIndex, value);
        }

        public void setDouble(int fieldIndex, double value) {
            this.fieldAccessor.setDouble(instance, fieldIndex, value);
        }

        public void setFloat(int fieldIndex, float value) {
            this.fieldAccessor.setFloat(instance, fieldIndex, value);
        }

        public void setChar(int fieldIndex, char value) {
            this.fieldAccessor.setChar(instance, fieldIndex, value);
        }

        public Object get(int fieldIndex) {
            return this.fieldAccessor.get(instance, fieldIndex);
        }

        public String getString(int fieldIndex) {
            return this.fieldAccessor.getString(instance, fieldIndex);
        }

        public char getChar(int fieldIndex) {
            return this.fieldAccessor.getChar(instance, fieldIndex);
        }

        public boolean getBoolean(int fieldIndex) {
            return this.fieldAccessor.getBoolean(instance, fieldIndex);
        }

        public byte getByte(int fieldIndex) {
            return this.fieldAccessor.getByte(instance, fieldIndex);
        }

        public short getShort(int fieldIndex) {
            return this.fieldAccessor.getShort(instance, fieldIndex);
        }

        public int getInt(int fieldIndex) {
            return this.fieldAccessor.getInt(instance, fieldIndex);
        }

        public long getLong(int fieldIndex) {
            return this.fieldAccessor.getLong(instance, fieldIndex);
        }

        public double getDouble(int fieldIndex) {
            return this.fieldAccessor.getFloat(instance, fieldIndex);
        }

        public float getFloat(int fieldIndex) {
            return this.fieldAccessor.getFloat(instance, fieldIndex);
        }

        public Object instance() {
            return instance;
        }
    }

    public static final class RECORD_CLASS_STREAM_END {
       public final int marker = -1;
    }

    public static final class RECORD_CLASS_BLOCK_END {
        public final int marker = -2;
    }

    public static final class RECORD_CLASS_GROUP_END {
        public final int marker = -3;
    }

    public static final class RECORD_CLASS_ITERATION_END {
        public final int marker = -3;
    }
}

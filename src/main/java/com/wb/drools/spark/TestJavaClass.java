package com.wb.drools.spark;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

/**
 * @author wenbao
 * @version 1.0.0
 * @email wenbao@yijiupi.com
 * @Description 通过.java文件生成对象
 * @createTime 14:32 2020/6/4
 */
public class TestJavaClass {
    public static void main(String[] args) {
        try {
            Object student = createStudent("Student", classString);
            assert student != null;
            Method setStudentIdMethod = student.getClass().getMethod("setStudentId", Integer.class);
            Method getStudentIdMethod = student.getClass().getMethod("getStudentId");
            if (setStudentIdMethod != null) {
                setStudentIdMethod.invoke(student, 1101);
                Object studentId = getStudentIdMethod.invoke(student);
                System.err.println("设置的studentId为：" + studentId.toString());
            }
        } catch (URISyntaxException | ClassNotFoundException | IllegalAccessException | InstantiationException | IOException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private static String classString = "" +
            "public class Student{                                  " +
            "       private Integer  studentId;                      " +
            "       public Integer getStudentId(){                   " +
            "           return this.studentId;                      " +
            "       }                                               " +
            "       public void setStudentId(Integer studentId){  " +
            "           this.studentId = studentId;                 " +
            "       }                                               " +
            "}                                                    ";

    public static Object createStudent(String tableName, String classString) throws URISyntaxException, ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, NoSuchMethodException, InvocationTargetException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);
        ClassJavaFileManager classJavaFileManager = new ClassJavaFileManager(standardFileManager);
        StringObject stringObject = new StringObject(new URI(tableName + ".java"), JavaFileObject.Kind.SOURCE, classString);
        JavaCompiler.CompilationTask task = compiler.getTask(null, classJavaFileManager, null, null, null, Collections.singletonList(stringObject));
        if (task.call()) {
            ClassJavaFileObject javaFileObject = classJavaFileManager.getClassJavaFileObject();
            ClassLoader classLoader = new MyClassLoader(javaFileObject);
            return classLoader.loadClass(tableName).newInstance();
//			Method getStudetnId = student.getClass().getMethod("getStudentId");
//			Object invoke = getStudetnId.invoke(student);
//			logger.info("class==={}", student);
        }
        return null;
    }

    /**
     * 自定义fileManager
     */
    static class ClassJavaFileManager extends ForwardingJavaFileManager {

        private ClassJavaFileObject classJavaFileObject;

        public ClassJavaFileManager(JavaFileManager fileManager) {
            super(fileManager);
        }

        public ClassJavaFileObject getClassJavaFileObject() {
            return classJavaFileObject;
        }

        /**
         * 这个方法一定要自定义
         */
        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling) throws IOException {
            return (classJavaFileObject = new ClassJavaFileObject(className, kind));
        }
    }

    /**
     * 存储源文件
     */
    static class StringObject extends SimpleJavaFileObject {

        private String content;

        public StringObject(URI uri, Kind kind, String content) {
            super(uri, kind);
            this.content = content;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return this.content;
        }
    }

    /**
     * class文件（不需要存到文件中）
     */
    static class ClassJavaFileObject extends SimpleJavaFileObject {

        ByteArrayOutputStream outputStream;

        public ClassJavaFileObject(String className, Kind kind) {
            super(URI.create(className + kind.extension), kind);
            this.outputStream = new ByteArrayOutputStream();
        }

        /**
         * 这个也要实现
         */
        @Override
        public OutputStream openOutputStream() throws IOException {
            return this.outputStream;
        }

        public byte[] getBytes() {
            return this.outputStream.toByteArray();
        }
    }

    /**
     * 自定义classloader
     */
    static class MyClassLoader extends ClassLoader {
        private ClassJavaFileObject stringObject;

        public MyClassLoader(ClassJavaFileObject stringObject) {
            this.stringObject = stringObject;
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            byte[] bytes = this.stringObject.getBytes();
            return defineClass(name, bytes, 0, bytes.length);
        }
    }
}

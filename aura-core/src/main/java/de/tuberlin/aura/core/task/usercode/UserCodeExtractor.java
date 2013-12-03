package de.tuberlin.aura.core.task.usercode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.bcel.Repository;
import org.apache.bcel.classfile.ConstantClass;
import org.apache.bcel.classfile.ConstantPool;
import org.apache.bcel.classfile.DescendingVisitor;
import org.apache.bcel.classfile.EmptyVisitor;
import org.apache.bcel.classfile.JavaClass;

import de.tuberlin.aura.core.common.utils.Compression;

public final class UserCodeExtractor { 
	
	//---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

	/**
	 * 
	 */
	private static final class DependencyEmitter extends EmptyVisitor {
	  
		public DependencyEmitter( final JavaClass javaClass ) {
			
			this.javaClass = javaClass;
		
			this.dependencies = new ArrayList<String>();
		}
	
		private final JavaClass javaClass;
		
		private final List<String> dependencies;
		
		@Override
		public void visitConstantClass( final ConstantClass obj ) {
			final ConstantPool cp = javaClass.getConstantPool();
			String bytes = obj.getBytes( cp );
			dependencies.add( bytes );
		}
	
		public static List<String> analyze( final Class<?> clazz ) {
			final JavaClass javaClass = Repository.lookupClass( clazz );
			final DependencyEmitter visitor = new DependencyEmitter( javaClass );
			( new DescendingVisitor( javaClass, visitor ) ).visit();
			return visitor.dependencies;
		}
	}

	//---------------------------------------------------
    // Public.
    //--------------------------------------------------- 

	public UserCode extractUserCodeClass( final Class<?> clazz ) {
		// sanity check.
		if( clazz == null )
			throw new IllegalArgumentException( "clazz == null" );
		
		if( clazz.isMemberClass() && !Modifier.isStatic( clazz.getModifiers() ) )
			throw new IllegalStateException();
		
		final List<String> dependencies = buildTransitiveDependencyClosure( clazz, new ArrayList<String>() );
		return new UserCode( clazz.getName(), dependencies, Compression.compress( loadByteCode( clazz ) ) );
	}

	//---------------------------------------------------
    // Private.
    //--------------------------------------------------- 
	
	private List<String> buildTransitiveDependencyClosure( final Class<?> clazz, 
			final List<String> globalDependencies ) {
		
		final String fullQualifiedPath = clazz.getCanonicalName();
		final List<String> levelDependencies = DependencyEmitter.analyze( clazz );		
		for( String dependency : levelDependencies ) {
			
			// TODO: make it flexible to provide more standard libraries.
			if( !dependency.contains( "java" ) &&
				!dependency.contains( "org/apache/log4j" ) &&
				!dependency.contains( "io/netty" ) &&
				!dependency.contains( "de/tuberlin/aura/core" ) ) { 			
				
				final String dp1 = dependency.replace( "/", "." );
				final String dp2 = dp1.replace( "$", "." );		 
				
				boolean isTransitiveEnclosingClass = false;
				for( final String dp : globalDependencies )
					if( dp.contains( dp2 ) ) {
						isTransitiveEnclosingClass = true;
						break;
					}
								
				if( !fullQualifiedPath.contains( dp2 ) && !isTransitiveEnclosingClass ) {
					globalDependencies.add( dp2 );
										
					final Class<?> dependencyClass; 
					try {
						dependencyClass = Class.forName( dp1, false, clazz.getClassLoader() );
					} catch( ClassNotFoundException e ) {
						throw new IllegalStateException( e );
					}
					
					if( !dependencyClass.isArray() && !dependencyClass.isPrimitive() )
						buildTransitiveDependencyClosure( dependencyClass, globalDependencies );
				}
			}
		}
			
		return globalDependencies;
	} 

	private byte[] loadByteCode( final Class<?> clazz ) {
		
		// TODO: a simpler way possible!!
		
		// TODO: handle JAR Files!!
		
		Class<?> enclosingClazz = clazz.getEnclosingClass();
		String topLevelClazzName = null;
		while( enclosingClazz != null ) {
			topLevelClazzName = enclosingClazz.getSimpleName();
			enclosingClazz = enclosingClazz.getEnclosingClass();
		}
		
		final StringTokenizer tokenizer = new StringTokenizer( clazz.getCanonicalName(), "." );
		final StringBuilder pathBuilder = new StringBuilder();
		
		boolean isClazzFilename = false; 
		while( tokenizer.hasMoreTokens() ) {
			final String token = tokenizer.nextToken(); 
			if( !token.equals( topLevelClazzName ) && !isClazzFilename )
				pathBuilder.append( token ).append( "/" );
			else {
				pathBuilder.append( token );
				if( tokenizer.hasMoreTokens() )
					pathBuilder.append( "$" );
				isClazzFilename = true;
			}
		}
		
		final String filePath = clazz.getProtectionDomain().getCodeSource().getLocation().getPath() + 
				pathBuilder.toString() + ".class";				
		final File clazzFile = new File( filePath.replace( "%20", " " ) );		

		FileInputStream fis = null;
		byte[] clazzData = null;
		try {
			fis = new FileInputStream( clazzFile );
			clazzData = new byte[(int) clazzFile.length()];
			fis.read( clazzData );
		} catch( FileNotFoundException e ) {
			throw new IllegalStateException( e );
		} catch( IOException e ) {
			throw new IllegalStateException( e );		
		} finally {
			try {
				if( fis != null )
					fis.close();
			} catch( IOException e ) {
				throw new IllegalStateException( e );
			}
		}
		
		return clazzData;
	}		
}

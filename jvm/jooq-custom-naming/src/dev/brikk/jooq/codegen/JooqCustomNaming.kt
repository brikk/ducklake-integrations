package dev.brikk.jooq.codegen

import org.jooq.codegen.DefaultGeneratorStrategy
import org.jooq.codegen.GeneratorStrategy
import org.jooq.meta.CatalogDefinition
import org.jooq.meta.ConstraintDefinition
import org.jooq.meta.Definition
import org.jooq.meta.EnumDefinition
import org.jooq.meta.IndexDefinition
import org.jooq.meta.SchemaDefinition
import org.jooq.meta.TableDefinition
import org.jooq.tools.StringUtils


open class JooqCustomNaming : DefaultGeneratorStrategy() {
    override fun getGlobalReferencesJavaClassName(container: Definition, objectType: Class<out Definition>): String {
        val typeName = when {
            ConstraintDefinition::class.java.isAssignableFrom(objectType) -> "Keys"
            EnumDefinition::class.java.isAssignableFrom(objectType) -> "Enums"
            IndexDefinition::class.java.isAssignableFrom(objectType) -> "Indexes"
            TableDefinition::class.java.isAssignableFrom(objectType) -> "Tables"
            else -> super.getGlobalReferencesJavaClassName(container, objectType)
        }
        val prefix = StringUtils.toCamelCase(container.outputName.replace(' ', '_').replace('-', '_').replace('.', '_'))
        val middle = if (prefix.endsWith("db", ignoreCase = true)) "" else "Db"
        val result = "${prefix}${middle}${typeName}"

        return result
    }

    override fun getJavaClassName(definition: Definition, mode: GeneratorStrategy.Mode): String {
        fun fixCase(ident: String): String {
            val result = StringBuilder()

            result.append(
                StringUtils.toCamelCase(
                    ident.replace(' ', '_').replace('-', '_').replace('.', '_')
                )
            )

            when (mode) {
                GeneratorStrategy.Mode.RECORD -> result.append("Record")
                GeneratorStrategy.Mode.DAO -> result.append("Dao")
                GeneratorStrategy.Mode.INTERFACE -> result.insert(0, "I")
                else -> {}
            }

            return result.toString()
        }

        val result = when {
            definition is CatalogDefinition && definition.isDefaultCatalog -> fixCase(definition.outputName) + "DefaultCatalog"
            definition is SchemaDefinition && definition.isDefaultSchema -> fixCase(definition.outputName) + "DefaultSchema"
            definition is TableDefinition && mode == GeneratorStrategy.Mode.DEFAULT -> fixCase(definition.outputName) + "Table"
            else -> super.getJavaClassName(definition, mode)
//            when (mode) {
//                GeneratorStrategy.Mode.POJO -> fixCase(definition.outputName) + "Pojo"
//                else -> super.getJavaClassName(definition, mode)
//            }
        }

        return result
    }

    override fun getJavaIdentifier(definition: Definition): String {
        return super.getJavaIdentifier(definition)
    }

    override fun getJavaPackageName(definition: Definition, mode: GeneratorStrategy.Mode): String{
        // put POJOs at the top level, since we only generate them for JPA
        return if (mode == GeneratorStrategy.Mode.POJO && definition !is CatalogDefinition && definition !is SchemaDefinition) {
            return targetPackage
        } else {
            super.getJavaPackageName(definition, mode)
        }
    }

}

open class JpaCustomNaming : JooqCustomNaming() {
    override fun getJavaPackageName(definition: Definition, mode: GeneratorStrategy.Mode): String{
        // put POJOs at the top level, since we only generate them for JPA
        return if (mode == GeneratorStrategy.Mode.POJO && definition !is CatalogDefinition && definition !is SchemaDefinition) {
            return targetPackage
        } else {
            super.getJavaPackageName(definition, mode)
        }
    }

}
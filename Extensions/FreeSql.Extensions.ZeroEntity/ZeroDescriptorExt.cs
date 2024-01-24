using System.Linq.Expressions;
using System.Reflection;

using static FreeSql.Extensions.ZeroEntity.TableDescriptor;

namespace FreeSql.Extensions.ZeroEntity
{
    public static class TableDescriptorExt
    {
        public static TableDescriptor FromType<T>(List<ColumnDescriptor>? columns = null, Func<PropertyInfo, bool>? filter = null, List<IndexDescriptor>? indexs = null) where T : class => FromType(typeof(T), columns, filter, indexs);
        public static TableDescriptor FromType(Type type, List<ColumnDescriptor>? columns = null, Func<PropertyInfo, bool>? filter = null, List<IndexDescriptor>? indexs = null)
        {
            var tableDesc = new TableDescriptor() { Name = type.Name };
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            if (tableAttr is not null)
            {
                tableDesc.AsTable = tableAttr.AsTable;
                tableDesc.DisableSyncStructure = tableAttr.DisableSyncStructure;
            }
            var descAttr = type.GetCustomAttribute<DescriptionAttribute>();
            tableDesc.Comment = descAttr?.Description;
            tableDesc.Columns.AddRange(ColumnDescriptorExt.FromType(type, columns, filter));
            tableDesc.Indexes.AddRange(IndexDescriptorExt.FromType(type, indexs));
            return tableDesc;
        }
    }
    public static class ColumnDescriptorExt
    {
        static Lazy<Dictionary<string, (PropertyInfo DescProp, MemberInfo? ColField)>> ColumnDescAttrMaps = new Lazy<Dictionary<string, (PropertyInfo, MemberInfo?)>>(() =>
        {
            var pubProps = typeof(ColumnAttribute).GetProperties(BindingFlags.Instance | BindingFlags.Public).Cast<MemberInfo>();
            var defProps = typeof(ColumnAttribute).GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
            var desProps = typeof(ColumnDescriptor).GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToDictionary(prop => prop.Name, prop => (prop,
                    defProps.FirstOrDefault(p => p.Name == $"_{prop.Name}") ??
                    pubProps.FirstOrDefault(p => p.Name == prop.Name)),
                StringComparer.CurrentCultureIgnoreCase);
            return desProps;
        });

        public static ColumnDescriptor FromProperty(PropertyInfo property, Dictionary<string, object?>? dict = null)
        {
            var colDesc = new ColumnDescriptor();
            colDesc.WithProperty(property);
            colDesc.WithAttribute(property.GetCustomAttribute<DescriptionAttribute>());
            colDesc.WithAttribute(property.GetCustomAttribute<ColumnAttribute>());
            colDesc.WithDictionary(dict);
            return colDesc;
        }
        public static ColumnDescriptor? FromDictionary(Dictionary<string, object> dict)
        {
            if (dict is null) return null;
            var colDesc = new ColumnDescriptor();
            foreach (var item in dict)
                if (ColumnDescAttrMaps.Value.TryGetValue(item.Key, out var pf))
                    pf.DescProp.SetValue(colDesc, Convert.ChangeType(item.Value, pf.DescProp.PropertyType));
            if (colDesc is { Name: null } or { MapType: null }) return null;
            return colDesc;
        }

        public static ColumnDescriptor WithProperty(this ColumnDescriptor @this, PropertyInfo property)
        {
            @this.Name = property.Name;
            @this.MapType = property.PropertyType;
            if (property.PropertyType.IsNullableType())
                @this.IsNullable = true;
            return @this;
        }
        public static ColumnDescriptor WithAttribute(this ColumnDescriptor @this, DescriptionAttribute? attribute)
        {
            if (attribute is { Description.Length: > 0 })
                @this.Comment = attribute.Description;
            return @this;
        }
        static Lazy<Func<ColumnAttribute, ColumnDescriptor, ColumnDescriptor>> WithAttributeFunc = new Lazy<Func<ColumnAttribute, ColumnDescriptor, ColumnDescriptor>>(() =>
        {
            var descType = typeof(ColumnDescriptor);
            var attrType = typeof(ColumnAttribute);
            var descParam = Expression.Parameter(descType, "desc");
            var attrParam = Expression.Parameter(attrType, "attr");
            var returnTarget = Expression.Label(descType, "ret");
            var exps = new List<Expression>();
            foreach (var maps in ColumnDescAttrMaps.Value)
            {
                var descProp = maps.Value.DescProp;
                var attrField = maps.Value.ColField;
                if (attrField is null) continue;
                if (attrField is FieldInfo fieldInfo)
                    exps.Add(Expression.IfThen(Expression.Equal(Expression.Equal(Expression.MakeMemberAccess(attrParam, fieldInfo), Expression.Constant(null)), Expression.Constant(false)),
                        Expression.Assign(Expression.MakeMemberAccess(descParam, descProp), Expression.Convert(Expression.MakeMemberAccess(attrParam, fieldInfo), maps.Value.DescProp.PropertyType))));
                else if (attrField is PropertyInfo propInfo)
                    exps.Add(Expression.IfThen(Expression.Equal(Expression.Equal(Expression.Convert(Expression.MakeMemberAccess(attrParam, propInfo), typeof(object)), Expression.Constant(null)), Expression.Constant(false)),
                        Expression.Assign(Expression.MakeMemberAccess(descParam, descProp), Expression.MakeMemberAccess(attrParam, propInfo))));
            }
            exps.AddRange(new Expression[] {
                Expression.Return(returnTarget, descParam),
                Expression.Label(returnTarget,Expression.Default(descType)),
            });
            return Expression.Lambda<Func<ColumnAttribute, ColumnDescriptor, ColumnDescriptor>>(Expression.Block(exps), new[] { attrParam, descParam }).Compile();
        });
        public static ColumnDescriptor WithAttribute(this ColumnDescriptor @this, ColumnAttribute? attribute)
        {
            if (attribute is not null)
                return WithAttributeFunc.Value(attribute, @this);
            return @this;
        }
        public static ColumnDescriptor WithDictionary(this ColumnDescriptor @this, Dictionary<string, object?>? dict)
        {
            if (dict is { Count: > 0 })
                foreach (var item in dict)
                    if (ColumnDescAttrMaps.Value.TryGetValue(item.Key, out var pf))
                        pf.DescProp.SetValue(@this, Convert.ChangeType(item.Value, pf.DescProp.PropertyType));
            return @this;
        }

        public static List<ColumnDescriptor> FromType<T>(List<ColumnDescriptor>? columns = null, Func<PropertyInfo, bool>? filter = null) where T : class => FromType(typeof(T), columns, filter);
        public static List<ColumnDescriptor> FromType(Type type, List<ColumnDescriptor>? columns = null, Func<PropertyInfo, bool>? filter = null)
        {
            var dict = new Dictionary<string, ColumnDescriptor>();
            var props = type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(prop =>
                    prop is { CanRead: true, CanWrite: true } &&
                    prop.GetCustomAttribute<ColumnAttribute>() is not { IsIgnore: true } &&
                    (filter == null ? true : filter(prop))
                ).GroupBy(prop => prop.DeclaringType).Reverse()
                .SelectMany(props => props.OrderBy(prop => prop.MetadataToken)).ToArray();
            foreach (var prop in props)
            {
                var col = FromProperty(prop);
                dict[col.Name] = col;
            }
            if (columns is { Count: > 0 })
                foreach (var col in columns)
                    dict[col.Name] = col;
            return dict.Values.ToList();
        }
    }
    public class IndexDescriptorExt : IndexDescriptor
    {
        static Lazy<Func<IndexAttribute, IndexDescriptor>> FromAttributeFunc = new Lazy<Func<IndexAttribute, IndexDescriptor>>(() =>
        {
            var descType = typeof(IndexDescriptor);
            var attrType = typeof(IndexAttribute);
            var descProps = descType.GetProperties();
            var attrProps = attrType.GetProperties();
            var parameter = Expression.Parameter(attrType);
            var returnTarget = Expression.Label(descType);
            var varRet = Expression.Variable(descType);
            var exps = new List<Expression>() { Expression.Assign(varRet, Expression.New(descType)) };
            foreach (var descProp in descProps)
            {
                var attrProp = attrProps.FirstOrDefault(p => p.Name == descProp.Name);
                if (attrProp is not null)
                    exps.Add(Expression.Assign(Expression.MakeMemberAccess(varRet, descProp), Expression.MakeMemberAccess(parameter, attrProp)));
            }
            exps.AddRange(new Expression[] {
                Expression.Return(returnTarget, varRet),
                Expression.Label(returnTarget,Expression.Default(descType)),
            });
            return Expression.Lambda<Func<IndexAttribute, IndexDescriptor>>(Expression.Block(new[] { varRet }, exps), new[] { parameter }).Compile();
        });
        public static List<IndexDescriptor> FromType<T>(List<IndexDescriptor>? indexs = null) where T : class => FromType(typeof(T), indexs);
        public static List<IndexDescriptor> FromType(Type type, List<IndexDescriptor>? indexs = null)
        {
            var dict = new Dictionary<string, IndexDescriptor>();
            var idxAttrs = type.GetCustomAttributes<IndexAttribute>();
            foreach (var idx in idxAttrs)
                dict[idx.Name] = FromAttributeFunc.Value(idx);
            if (indexs is { Count: > 0 })
                foreach (var idx in indexs)
                    dict[idx.Name] = idx;
            return dict.Values.ToList();
        }
    }
}

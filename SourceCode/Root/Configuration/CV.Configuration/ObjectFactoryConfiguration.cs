﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

using CV.Global;

namespace CV.Configuration
{
    /// <summary>
    /// This class contains the collection of configuration settings for the ObjectFactoryElements
    /// </summary>
    public class ObjectFactoryCollection : ConfigurationElementCollection
    {
        /// <summary>
        /// Returns the element's name
        /// </summary>
        protected override string ElementName
        {
            get { return Constants.ObjectFactory; }
        }

        /// <summary>
        /// Returns the element for the given index
        /// </summary>
        /// <param name="index">Index within the collection</param>
        /// <returns></returns>
        public ObjectFactoryElement this[int index]
        {
            get
            {
                return (ObjectFactoryElement)BaseGet(index);
            }
        }

        /// <summary>
        /// Returns the element for the given key
        /// </summary>
        /// <param name="keyName">Name of key within the collection</param>
        /// <returns></returns>
        public new ObjectFactoryElement this[string keyName]
        {
            get
            {
                return (ObjectFactoryElement)BaseGet(keyName);
            }
        }

        /// <summary>
        /// Returns the ConfigurationElementCollectionType for the collection
        /// </summary>
        public override ConfigurationElementCollectionType CollectionType
        {
            get { return ConfigurationElementCollectionType.BasicMap; }
        }

        /// <summary>
        /// Returns the newly instantiated ObjectFactoryElement
        /// </summary>
        /// <returns></returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new ObjectFactoryElement();
        }

        /// <summary>
        /// Returns the key  to the given configuration element
        /// </summary>
        /// <param name="element">Configuration Element</param>
        /// <returns></returns>
        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((ObjectFactoryElement)element).ObjectKey;
        }
    }

    /// <summary>
    /// Returns the Configuration Section class for the Object Factory
    /// </summary>
    public class ObjectFactoryConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Returns name of the configuration section
        /// </summary>
        public const string ConfigSectionName = Constants.ObjectFactories;
        
        /// <summary>
        /// Returns the configuration section
        /// </summary>
        /// <param name="isRequired">Indicates whether the given section is required</param>
        /// <exception cref="ExceptionMgr">Thrown when config section not found and is required.</exception>
        /// <returns>ObjectFactoryConfiguration object</returns>
        public static ObjectFactoryConfiguration GetSection(bool isRequired = true)
        {
            ObjectFactoryConfiguration ofc = (ObjectFactoryConfiguration)ConfigurationManager.GetSection(ConfigSectionName);
            if (isRequired && ofc.ObjectFactories.Count == 0)
                throw new ExceptionMgr(typeof(ObjectFactoryConfiguration).ToString()
                , new ArgumentNullException(
                    string.Format("The section key: {0} was not found.", ConfigSectionName))); 
            return ofc;
        }

        /// <summary>
        /// Returns the collection of ObjectFactories
        /// </summary>
        [ConfigurationProperty("", IsDefaultCollection = true)]
        public ObjectFactoryCollection ObjectFactories
        {
            get { return (ObjectFactoryCollection)base[""]; }
            set { base[""] = value; }
        }

        /// <summary>
        /// Returns the element for the given object key
        /// </summary>
        /// <param name="objectKey">Identifies the key with the collection</param>
        /// <param name="isRequired">Indicates whether the given section is required</param>
        /// <exception cref="ExceptionMgr">Thrown when config section not found and is required.</exception>
        /// <returns>ObjectFactoryElement object</returns>
        public ObjectFactoryElement GetFactoryObject(string objectKey, bool isRequired = true)
        {
            ObjectFactoryElement ofe = ObjectFactories.Cast<ObjectFactoryElement>()
                .FirstOrDefault(attribute => attribute.ObjectKey == objectKey);
            if (isRequired && ofe == null)
                throw new ExceptionMgr(typeof(ObjectFactoryConfiguration).ToString()
                      , new ArgumentNullException(
                          string.Format("Key: {0} not found in ObjectFactories section", objectKey)));
            return ofe;
        }

    }

    /// <summary>
    /// Returns the configuration element for object factory configuration
    /// </summary>
    public class ObjectFactoryElement : ConfigurationElement
    {
        /// <summary>
        /// Returns the key to the element
        /// </summary>
        [ConfigurationProperty(Constants.ObjectKey, IsRequired = true)]
        public string ObjectKey
        {
            get { return this[Constants.ObjectKey] as string; }
            set { this[Constants.ObjectKey] = value; }
        }

        /// <summary>
        /// Returns the value of the attribute (Class name of the object)
        /// </summary>
        [ConfigurationProperty(Constants.ObjectClass, IsRequired = true)]
        public string ObjectClass
        {
            get { return this[Constants.ObjectClass] as string; }
            set { this[Constants.ObjectClass] = value; }
        }

        /// <summary>
        /// Returns the value of the attribute (Assembly name that contains the object)
        /// </summary>
        [ConfigurationProperty(Constants.AssemblyName, IsRequired = true)]
        public string AssemblyName
        {
            get { return this[Constants.AssemblyName] as string; }
            set { this[Constants.AssemblyName] = value; }
        }

        /// <summary>
        /// Returns the value of the attribute (The path of the assembly dll)
        /// </summary>
        [ConfigurationProperty(Constants.AssemblyPath, IsRequired = true)]
        public string AssemblyPath
        {
            get { return this[Constants.AssemblyPath] as string; }
            set { this[Constants.AssemblyPath] = value; }
        }
    }
}

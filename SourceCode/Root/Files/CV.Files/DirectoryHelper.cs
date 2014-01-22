using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CV.Files
{
    public class DirectoryHelper
    {
        public static void VerifyDirectory(string path)
        {
            if (Directory.Exists(path))
                return;
            StringBuilder subDir = new StringBuilder();
            foreach (string pathPart in path.Split(new char[] { '\\' }).ToList())
            {
                if (pathPart.Contains(":"))
                {
                    subDir.Append(pathPart);
                    continue;
                }
                else
                {
                    subDir.Append("\\" + pathPart);
                    if (!Directory.Exists(subDir.ToString()))
                        Directory.CreateDirectory(subDir.ToString());
                }
            }
        }
    }
}

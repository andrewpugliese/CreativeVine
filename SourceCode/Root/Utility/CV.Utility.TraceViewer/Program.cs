//
// Copyright (c) 2011 Base One International Corporation. All rights reserved.
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace CV.TraceViewer
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new TraceViewer());
        }
    }
}

 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Power.JJ.APP
{
    /// <summary>
    ///  审批流所需session  
    /// </summary>
    public class TSession 
    {
        /// <summary>
        /// 业务域ID
        /// </summary>
        public string BizAreaId { get; set; }


        /// <summary>
        /// 流程起草时的 epsProjectId，也就是 pwf_infos.EpsProjectId
        /// </summary>
        public string BaseEpsProjectId { get; set; }

        /// <summary>
        /// 当前所属eps
        /// </summary>
        public string EpsProjectId { get; set; }

        /// <summary>
        /// 当前所属eps
        /// </summary>
        public string EpsProjectCode { get; set; }


        /// <summary>
        /// 当前所属eps
        /// </summary>
        public string EpsProjectName { get; set; }


        /// <summary>
        /// 当前human
        /// </summary>
        public string HumanId { get; set; }

        /// <summary>
        /// 当前用户
        /// </summary>
        public string HumanCode { get; set; }

        /// <summary>
        /// 当前用户
        /// </summary>
        public string HumanName { get; set; }

        /// <summary>
        /// 岗位部门ID
        /// </summary>
        public string DeptPositionID { get; set; }

        /// <summary>
        /// 岗位部门名称
        /// </summary>
        public string DeptPositionName { get; set; }
        /// <summary>
        /// 岗位部门模式
        /// </summary>
        public string  SourceMode { get; set; }

         
    }
}

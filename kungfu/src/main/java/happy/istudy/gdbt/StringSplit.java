package happy.istudy.gdbt;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/21.
 */
public class StringSplit {
    static public List<String> splitSimpleString(String source, char gap)
    {
        List<String> result = new LinkedList<String>();
        if (source == null) return result;
        char[] sourceChars = source.toCharArray();
        int startIndex = 0, index = -1;
        while (index++ != sourceChars.length)
        {
            if (index == sourceChars.length || sourceChars[index] == gap)
            {
                char[] section = new char[index - startIndex];
                System.arraycopy(sourceChars, startIndex,
                        section, 0, index - startIndex);
                result.add(String.valueOf(section));
                startIndex = index + 1;
            }
        }
        return result;
    }
}
